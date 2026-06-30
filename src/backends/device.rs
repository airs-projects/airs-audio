use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use futures::Sink;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::*;

pub(crate) fn list() -> Result<AudioDeviceList> {
    let default_input_name = InputDevice::default().and_then(|input| input.name()).ok();
    let default_output_name = OutputDevice::default()
        .and_then(|output| output.name())
        .ok();

    let inputs = InputDevice::names()?
        .into_iter()
        .map(|name| AudioDeviceInfo {
            is_default: default_input_name.as_deref() == Some(name.as_str()),
            name,
        })
        .collect();
    let outputs = OutputDevice::names()?
        .into_iter()
        .map(|name| AudioDeviceInfo {
            is_default: default_output_name.as_deref() == Some(name.as_str()),
            name,
        })
        .collect();

    Ok(AudioDeviceList { inputs, outputs })
}

pub(crate) fn stream(
    device_name: Option<&str>,
    sample_rate: Option<u32>,
    channels: Option<u16>,
    buffer_size: Option<u32>,
) -> Result<(BoxedAudioStream, cpal::Stream)> {
    let input = match device_name {
        Some(device_name) => InputDevice::named(device_name)?,
        None => InputDevice::default()?,
    };
    let config = input.device.default_input_config()?;
    let sample_format = config.sample_format();
    let mut stream_config = config.config();
    if let Some(sample_rate) = sample_rate {
        stream_config.sample_rate = cpal::SampleRate(sample_rate);
    }
    if let Some(channels) = channels {
        stream_config.channels = channels;
    }
    if let Some(buffer_size) = buffer_size {
        stream_config.buffer_size = cpal::BufferSize::Fixed(buffer_size);
    }

    let channels = stream_config.channels;
    let sample_rate = stream_config.sample_rate.0;
    let (sender, receiver) = mpsc::unbounded_channel();
    let stream = build_input_stream(
        &input.device,
        &stream_config,
        sample_format,
        channels,
        sample_rate,
        sender,
    )?;

    Ok((
        Box::pin(DeviceAudioStream {
            frames: UnboundedReceiverStream::new(receiver),
        }),
        stream,
    ))
}

pub(crate) fn sink(
    device_name: Option<&str>,
    sample_rate: Option<u32>,
    channels: Option<u16>,
    buffer_size: Option<u32>,
) -> Result<BoxedAudioSink> {
    Ok(Box::pin(DeviceAudioSink {
        device_name: device_name.map(str::to_owned),
        sample_rate,
        channels,
        buffer_size,
        state: None,
    }))
}

struct InputDevice {
    device: cpal::Device,
}

impl InputDevice {
    fn default() -> Result<Self> {
        let host = cpal::default_host();
        let device = host
            .default_input_device()
            .ok_or(AudioError::DefaultInputDeviceNotFound)?;

        Ok(Self { device })
    }

    fn named(name: impl AsRef<str>) -> Result<Self> {
        let name = name.as_ref();
        let host = cpal::default_host();

        for device in host.input_devices()? {
            if device.name()? == name {
                return Ok(Self { device });
            }
        }

        Err(AudioError::DeviceNotFound(name.to_string()))
    }

    fn names() -> Result<Vec<String>> {
        cpal::default_host()
            .input_devices()?
            .map(|device| device.name().map_err(AudioError::from))
            .collect()
    }

    fn name(&self) -> Result<String> {
        self.device.name().map_err(AudioError::from)
    }
}

struct OutputDevice {
    device: cpal::Device,
}

impl OutputDevice {
    fn default() -> Result<Self> {
        let host = cpal::default_host();
        let device = host
            .default_output_device()
            .ok_or(AudioError::DefaultOutputDeviceNotFound)?;

        Ok(Self { device })
    }

    fn named(name: impl AsRef<str>) -> Result<Self> {
        let name = name.as_ref();
        let host = cpal::default_host();

        for device in host.output_devices()? {
            if device.name()? == name {
                return Ok(Self { device });
            }
        }

        Err(AudioError::DeviceNotFound(name.to_string()))
    }

    fn names() -> Result<Vec<String>> {
        cpal::default_host()
            .output_devices()?
            .map(|device| device.name().map_err(AudioError::from))
            .collect()
    }

    fn name(&self) -> Result<String> {
        self.device.name().map_err(AudioError::from)
    }
}

struct DeviceAudioStream {
    frames: UnboundedReceiverStream<Result<AudioFrame>>,
}

impl Unpin for DeviceAudioStream {}

impl Stream for DeviceAudioStream {
    type Item = Result<AudioFrame>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.frames).poll_next(context)
    }
}

struct DeviceAudioSink {
    device_name: Option<String>,
    sample_rate: Option<u32>,
    channels: Option<u16>,
    buffer_size: Option<u32>,
    state: Option<DeviceSinkState>,
}

struct DeviceSinkState {
    _stream: cpal::Stream,
    playback: Arc<Mutex<PlaybackState>>,
    output_channels: u16,
    output_sample_rate: u32,
}

struct PlaybackState {
    samples: VecDeque<f32>,
    waker: Option<Waker>,
}

impl Unpin for DeviceAudioSink {}

impl DeviceAudioSink {
    fn send_frame(&mut self, frame: AudioFrame) -> Result<()> {
        validate_frame(&frame)?;

        if self.state.is_none() {
            let device = match &self.device_name {
                Some(name) => OutputDevice::named(name)?,
                None => OutputDevice::default()?,
            };
            let config = device.device.default_output_config()?;
            let sample_format = config.sample_format();
            let mut stream_config = config.config();
            if let Some(sample_rate) = self.sample_rate {
                stream_config.sample_rate = cpal::SampleRate(sample_rate);
            }
            if let Some(channels) = self.channels {
                stream_config.channels = channels;
            }
            if let Some(buffer_size) = self.buffer_size {
                stream_config.buffer_size = cpal::BufferSize::Fixed(buffer_size);
            }
            let output_channels = stream_config.channels;
            let output_sample_rate = stream_config.sample_rate.0;
            let playback = Arc::new(Mutex::new(PlaybackState {
                samples: VecDeque::new(),
                waker: None,
            }));
            let stream = build_output_stream(
                &device.device,
                &stream_config,
                sample_format,
                playback.clone(),
            )?;

            enqueue_output_frame(&playback, &frame, output_channels, output_sample_rate)?;
            stream.play()?;

            self.state = Some(DeviceSinkState {
                _stream: stream,
                playback,
                output_channels,
                output_sample_rate,
            });

            return Ok(());
        }

        let state = self
            .state
            .as_ref()
            .expect("device sink state initialized above");
        enqueue_output_frame(
            &state.playback,
            &frame,
            state.output_channels,
            state.output_sample_rate,
        )
    }
}

impl Sink<AudioFrame> for DeviceAudioSink {
    type Error = AudioError;

    fn poll_ready(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: AudioFrame) -> Result<()> {
        self.send_frame(item)
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<()>> {
        if let Some(state) = &self.state
            && !playback_is_empty(&state.playback, Some(context.waker().clone()))
        {
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }
}

fn enqueue_output_frame(
    state: &Arc<Mutex<PlaybackState>>,
    frame: &AudioFrame,
    output_channels: u16,
    output_sample_rate: u32,
) -> Result<()> {
    validate_frame(frame)?;
    let samples = prepare_output_samples(
        &frame.samples,
        frame.channels as usize,
        frame.sample_rate,
        output_channels as usize,
        output_sample_rate,
    );
    let mut state = state
        .lock()
        .expect("audio output playback state lock poisoned");

    state.samples.extend(samples);
    Ok(())
}

fn playback_is_empty(state: &Arc<Mutex<PlaybackState>>, waker: Option<Waker>) -> bool {
    let mut state = state
        .lock()
        .expect("audio output playback state lock poisoned");

    if state.samples.is_empty() {
        true
    } else {
        state.waker = waker;
        false
    }
}

fn build_output_stream(
    device: &cpal::Device,
    config: &cpal::StreamConfig,
    sample_format: cpal::SampleFormat,
    state: Arc<Mutex<PlaybackState>>,
) -> Result<cpal::Stream> {
    let err_fn = move |error: cpal::StreamError| {
        eprintln!("audio output stream error: {error}");
    };

    macro_rules! stream {
        ($sample:ty, $convert:path) => {{
            let state = state.clone();
            device.build_output_stream(
                config,
                move |data: &mut [$sample], _| write_output_samples(data, &state, $convert),
                err_fn,
                None,
            )?
        }};
    }

    let stream = match sample_format {
        cpal::SampleFormat::F32 => stream!(f32, f32_to_f32),
        cpal::SampleFormat::F64 => stream!(f64, f32_to_f64),
        cpal::SampleFormat::I8 => stream!(i8, f32_to_i8),
        cpal::SampleFormat::I16 => stream!(i16, f32_to_i16),
        cpal::SampleFormat::I32 => stream!(i32, f32_to_i32),
        cpal::SampleFormat::I64 => stream!(i64, f32_to_i64),
        cpal::SampleFormat::U8 => stream!(u8, f32_to_u8),
        cpal::SampleFormat::U16 => stream!(u16, f32_to_u16),
        cpal::SampleFormat::U32 => stream!(u32, f32_to_u32),
        cpal::SampleFormat::U64 => stream!(u64, f32_to_u64),
        sample_format => {
            return Err(invalid_input(format!(
                "unsupported output sample format: {sample_format}"
            )));
        }
    };

    Ok(stream)
}

fn write_output_samples<T>(
    output: &mut [T],
    state: &Arc<Mutex<PlaybackState>>,
    convert: fn(f32) -> T,
) {
    let mut state = state
        .lock()
        .expect("audio output playback state lock poisoned");

    for sample in output {
        *sample = convert(state.samples.pop_front().unwrap_or(0.0));
    }

    if state.samples.is_empty()
        && let Some(waker) = state.waker.take()
    {
        waker.wake();
    }
}

fn build_input_stream(
    device: &cpal::Device,
    config: &cpal::StreamConfig,
    sample_format: cpal::SampleFormat,
    channels: u16,
    sample_rate: u32,
    sender: mpsc::UnboundedSender<Result<AudioFrame>>,
) -> Result<cpal::Stream> {
    let error_sender = sender.clone();
    let err_fn = move |error: cpal::StreamError| {
        let _ = error_sender.send(Err(AudioError::InputStream(error.to_string())));
    };

    macro_rules! stream {
        ($sample:ty, $convert:path) => {{
            let sender = sender.clone();
            device.build_input_stream(
                config,
                move |data: &[$sample], _| {
                    let samples = convert_samples(data, $convert);
                    let frame = AudioFrame {
                        samples,
                        channels,
                        sample_rate,
                    };
                    let _ = sender.send(Ok(frame));
                },
                err_fn,
                None,
            )?
        }};
    }

    let stream = match sample_format {
        cpal::SampleFormat::F32 => stream!(f32, f32_to_f32),
        cpal::SampleFormat::F64 => stream!(f64, f64_to_f32),
        cpal::SampleFormat::I8 => stream!(i8, i8_to_f32),
        cpal::SampleFormat::I16 => stream!(i16, i16_to_f32),
        cpal::SampleFormat::I32 => stream!(i32, i32_to_f32),
        cpal::SampleFormat::I64 => stream!(i64, i64_to_f32),
        cpal::SampleFormat::U8 => stream!(u8, u8_to_f32),
        cpal::SampleFormat::U16 => stream!(u16, u16_to_f32),
        cpal::SampleFormat::U32 => stream!(u32, u32_to_f32),
        cpal::SampleFormat::U64 => stream!(u64, u64_to_f32),
        sample_format => {
            return Err(invalid_input(format!(
                "unsupported input sample format: {sample_format}"
            )));
        }
    };

    Ok(stream)
}

fn convert_samples<T>(input: &[T], convert: fn(T) -> f32) -> Vec<f32>
where
    T: Copy,
{
    input
        .iter()
        .map(|sample| convert(*sample).clamp(-1.0, 1.0))
        .collect()
}

fn prepare_output_samples(
    samples: &[f32],
    input_channels: usize,
    input_sample_rate: u32,
    output_channels: usize,
    output_sample_rate: u32,
) -> Vec<f32> {
    let remixed = remix_channels(samples, input_channels, output_channels);

    if input_sample_rate == output_sample_rate {
        remixed
    } else {
        resample_linear(
            &remixed,
            output_channels,
            input_sample_rate,
            output_sample_rate,
        )
    }
}

fn f32_to_f32(sample: f32) -> f32 {
    sample
}

fn f32_to_f64(sample: f32) -> f64 {
    sample as f64
}

fn f32_to_i8(sample: f32) -> i8 {
    (sample.clamp(-1.0, 1.0) * i8::MAX as f32) as i8
}

fn f32_to_i16(sample: f32) -> i16 {
    (sample.clamp(-1.0, 1.0) * i16::MAX as f32) as i16
}

fn f32_to_i32(sample: f32) -> i32 {
    (sample.clamp(-1.0, 1.0) * i32::MAX as f32) as i32
}

fn f32_to_i64(sample: f32) -> i64 {
    (sample.clamp(-1.0, 1.0) * i64::MAX as f32) as i64
}

fn f32_to_u8(sample: f32) -> u8 {
    ((sample.clamp(-1.0, 1.0) + 1.0) * 0.5 * u8::MAX as f32) as u8
}

fn f32_to_u16(sample: f32) -> u16 {
    ((sample.clamp(-1.0, 1.0) + 1.0) * 0.5 * u16::MAX as f32) as u16
}

fn f32_to_u32(sample: f32) -> u32 {
    ((sample.clamp(-1.0, 1.0) + 1.0) * 0.5 * u32::MAX as f32) as u32
}

fn f32_to_u64(sample: f32) -> u64 {
    ((sample.clamp(-1.0, 1.0) + 1.0) * 0.5 * u64::MAX as f32) as u64
}

fn f64_to_f32(sample: f64) -> f32 {
    sample as f32
}

fn i8_to_f32(sample: i8) -> f32 {
    sample as f32 / i8::MAX as f32
}

fn i16_to_f32(sample: i16) -> f32 {
    sample as f32 / i16::MAX as f32
}

fn i32_to_f32(sample: i32) -> f32 {
    sample as f32 / i32::MAX as f32
}

fn i64_to_f32(sample: i64) -> f32 {
    sample as f32 / i64::MAX as f32
}

fn u8_to_f32(sample: u8) -> f32 {
    sample as f32 / u8::MAX as f32 * 2.0 - 1.0
}

fn u16_to_f32(sample: u16) -> f32 {
    sample as f32 / u16::MAX as f32 * 2.0 - 1.0
}

fn u32_to_f32(sample: u32) -> f32 {
    sample as f32 / u32::MAX as f32 * 2.0 - 1.0
}

fn u64_to_f32(sample: u64) -> f32 {
    sample as f32 / u64::MAX as f32 * 2.0 - 1.0
}
