use std::fmt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use cpal::traits::StreamTrait;
use futures::Sink;
use symphonia::core::errors::Error as SymphoniaError;
use tokio_stream::Stream;

#[path = "lib/device.rs"]
mod device;
#[path = "lib/file.rs"]
mod file;

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub type Result<T> = std::result::Result<T, AudioError>;
pub type BoxedAudioStream = Pin<Box<dyn Stream<Item = Result<AudioFrame>> + Send>>;
pub type BoxedAudioSink = Pin<Box<dyn Sink<AudioFrame, Error = AudioError> + Send>>;
type StreamOpener = Box<
    dyn FnOnce(
            Option<u32>,
            Option<u16>,
            Option<u32>,
        ) -> Result<(BoxedAudioStream, Option<cpal::Stream>)>
        + Send,
>;
type SinkOpener =
    Box<dyn FnOnce(Option<u32>, Option<u16>, Option<u32>) -> Result<BoxedAudioSink> + Send>;

#[derive(Clone, Debug, PartialEq)]
pub struct AudioFrame {
    pub samples: Vec<f32>,
    pub channels: u16,
    pub sample_rate: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum AudioError {
    #[error("audio stream not found")]
    AudioStreamNotFound,
    #[error("default input device not found")]
    DefaultInputDeviceNotFound,
    #[error("default output device not found")]
    DefaultOutputDeviceNotFound,
    #[error("audio device not found: {0}")]
    DeviceNotFound(String),
    #[error("device name error: {0}")]
    DeviceName(String),
    #[error("audio device error: {0}")]
    Devices(String),
    #[error("default stream config error: {0}")]
    DefaultStreamConfig(String),
    #[error("audio input stream error: {0}")]
    BuildStream(String),
    #[error("audio stream playback error: {0}")]
    PlayStream(String),
    #[error("audio input stream error: {0}")]
    InputStream(String),
    #[error("audio output error: {0}")]
    Output(String),
    #[error("audio encode error: {0}")]
    Encode(String),
    #[error("control signal error: {0}")]
    Control(String),
    #[error("number conversion error: {0}")]
    NumberConversion(String),
    #[error("{0}")]
    InvalidInput(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("audio decode error: {0}")]
    Symphonia(String),
}

impl From<cpal::DeviceNameError> for AudioError {
    fn from(error: cpal::DeviceNameError) -> Self {
        Self::DeviceName(error.to_string())
    }
}

impl From<cpal::DevicesError> for AudioError {
    fn from(error: cpal::DevicesError) -> Self {
        Self::Devices(error.to_string())
    }
}

impl From<cpal::DefaultStreamConfigError> for AudioError {
    fn from(error: cpal::DefaultStreamConfigError) -> Self {
        Self::DefaultStreamConfig(error.to_string())
    }
}

impl From<cpal::BuildStreamError> for AudioError {
    fn from(error: cpal::BuildStreamError) -> Self {
        Self::BuildStream(error.to_string())
    }
}

impl From<cpal::PlayStreamError> for AudioError {
    fn from(error: cpal::PlayStreamError) -> Self {
        Self::PlayStream(error.to_string())
    }
}

impl From<cpal::PauseStreamError> for AudioError {
    fn from(error: cpal::PauseStreamError) -> Self {
        Self::PlayStream(error.to_string())
    }
}

impl From<std::num::TryFromIntError> for AudioError {
    fn from(error: std::num::TryFromIntError) -> Self {
        Self::NumberConversion(error.to_string())
    }
}

impl From<SymphoniaError> for AudioError {
    fn from(error: SymphoniaError) -> Self {
        Self::Symphonia(error.to_string())
    }
}

pub struct AudioDeviceInfo {
    pub name: String,
    pub is_default: bool,
}

pub struct AudioDeviceList {
    pub inputs: Vec<AudioDeviceInfo>,
    pub outputs: Vec<AudioDeviceInfo>,
}

pub fn list_audio_devices() -> Result<AudioDeviceList> {
    device::list()
}

pub struct AudioStream {
    open: Option<StreamOpener>,
    sample_rate: Option<u32>,
    channels: Option<u16>,
    buffer_size: Option<u32>,
    stream: Option<BoxedAudioStream>,
    device_stream: Option<cpal::Stream>,
}

impl AudioStream {
    pub fn from_file(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        Self::new(Box::new(move |_, _, _| Ok((file::stream(&path)?, None))))
    }

    pub fn from_device(device_name: Option<String>) -> Self {
        Self::new(Box::new(move |sample_rate, channels, buffer_size| {
            device::stream(device_name.as_deref(), sample_rate, channels, buffer_size)
                .map(|(stream, device_stream)| (stream, Some(device_stream)))
        }))
    }

    pub fn from_named_device(device_name: impl Into<String>) -> Self {
        Self::from_device(Some(device_name.into()))
    }

    fn new(open: StreamOpener) -> Self {
        Self {
            open: Some(open),
            sample_rate: None,
            channels: None,
            buffer_size: None,
            stream: None,
            device_stream: None,
        }
    }

    pub fn sample_rate(mut self, rate: u32) -> Self {
        self.sample_rate = Some(rate);
        self
    }

    pub fn channels(mut self, n: u16) -> Self {
        self.channels = Some(n);
        self
    }

    pub fn buffer_size(mut self, size: u32) -> Self {
        self.buffer_size = Some(size);
        self
    }

    /// Start the device stream (no-op for file sources).
    pub fn start(&mut self) -> Result<()> {
        self.ensure_stream();
        if let Some(stream) = &self.device_stream {
            stream.play().map_err(AudioError::from)
        } else {
            Ok(())
        }
    }

    /// Pause the device stream (no-op for file sources).
    pub fn stop(&mut self) -> Result<()> {
        if let Some(stream) = &self.device_stream {
            stream.pause().map_err(AudioError::from)
        } else {
            Ok(())
        }
    }

    fn ensure_stream(&mut self) -> &mut BoxedAudioStream {
        if self.stream.is_none() {
            let open = self.open.take().expect("audio stream opener is available");
            match open(self.sample_rate, self.channels, self.buffer_size) {
                Ok((stream, device_stream)) => {
                    self.stream = Some(stream);
                    self.device_stream = device_stream;
                }
                Err(error) => {
                    self.stream = Some(Box::pin(tokio_stream::iter(vec![Err(error)])));
                }
            }
        }

        self.stream.as_mut().expect("audio stream is initialized")
    }
}

impl Stream for AudioStream {
    type Item = Result<AudioFrame>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.ensure_stream().as_mut().poll_next(context)
    }
}

pub struct AudioSink {
    open: Option<SinkOpener>,
    sample_rate: Option<u32>,
    channels: Option<u16>,
    buffer_size: Option<u32>,
    sink: Option<BoxedAudioSink>,
}

impl AudioSink {
    pub fn to_file(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        Self::new(Box::new(move |sample_rate, channels, _| {
            file::sink(&path, sample_rate, channels)
        }))
    }

    pub fn to_device(device_name: Option<String>) -> Self {
        Self::new(Box::new(move |sample_rate, channels, buffer_size| {
            device::sink(device_name.as_deref(), sample_rate, channels, buffer_size)
        }))
    }

    pub fn to_named_device(device_name: impl Into<String>) -> Self {
        Self::to_device(Some(device_name.into()))
    }

    fn new(open: SinkOpener) -> Self {
        Self {
            open: Some(open),
            sample_rate: None,
            channels: None,
            buffer_size: None,
            sink: None,
        }
    }

    pub fn sample_rate(mut self, rate: u32) -> Self {
        self.sample_rate = Some(rate);
        self
    }

    pub fn channels(mut self, n: u16) -> Self {
        self.channels = Some(n);
        self
    }

    pub fn buffer_size(mut self, size: u32) -> Self {
        self.buffer_size = Some(size);
        self
    }

    fn ensure_sink(&mut self) -> Result<&mut BoxedAudioSink> {
        if self.sink.is_none() {
            let open = self.open.take().expect("audio sink opener is available");
            self.sink = Some(open(self.sample_rate, self.channels, self.buffer_size)?);
        }

        Ok(self.sink.as_mut().expect("audio sink is initialized"))
    }
}

impl Sink<AudioFrame> for AudioSink {
    type Error = AudioError;

    fn poll_ready(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<()>> {
        match self.ensure_sink() {
            Ok(sink) => sink.as_mut().poll_ready(context),
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: AudioFrame) -> Result<()> {
        self.ensure_sink()?.as_mut().start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<()>> {
        match self.ensure_sink() {
            Ok(sink) => sink.as_mut().poll_flush(context),
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<()>> {
        match self.ensure_sink() {
            Ok(sink) => sink.as_mut().poll_close(context),
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

fn validate_frame(frame: &AudioFrame) -> Result<()> {
    if frame.channels == 0 {
        return Err(invalid_input("audio channel count is missing"));
    }

    if frame.sample_rate == 0 {
        return Err(invalid_input("audio sample rate is missing"));
    }

    if frame.samples.len() % frame.channels as usize != 0 {
        return Err(invalid_input("audio samples are not aligned to channels"));
    }

    Ok(())
}

fn validate_frame_format(channels: u16, sample_rate: u32, frame: &AudioFrame) -> Result<()> {
    if frame.channels != channels || frame.sample_rate != sample_rate {
        return Err(invalid_input("audio frame format changed"));
    }

    Ok(())
}

fn remix_channels(samples: &[f32], input_channels: usize, output_channels: usize) -> Vec<f32> {
    let input_frames = samples.len() / input_channels;
    let mut output = Vec::with_capacity(input_frames * output_channels);

    for frame in samples.chunks_exact(input_channels) {
        if input_channels == output_channels {
            output.extend_from_slice(frame);
        } else if output_channels == 1 {
            let sum: f32 = frame.iter().copied().sum();
            output.push(sum / input_channels as f32);
        } else {
            for channel in 0..output_channels {
                output.push(frame[channel.min(input_channels - 1)]);
            }
        }
    }

    output
}

fn resample_linear(
    samples: &[f32],
    channels: usize,
    input_sample_rate: u32,
    output_sample_rate: u32,
) -> Vec<f32> {
    let input_frames = samples.len() / channels;
    if input_frames == 0 {
        return Vec::new();
    }

    let output_frames =
        (input_frames as u64 * output_sample_rate as u64 / input_sample_rate as u64) as usize;
    let mut output = Vec::with_capacity(output_frames * channels);
    let ratio = input_sample_rate as f64 / output_sample_rate as f64;

    for output_frame in 0..output_frames {
        let source_position = output_frame as f64 * ratio;
        let source_frame = source_position.floor() as usize;
        let next_frame = (source_frame + 1).min(input_frames - 1);
        let fraction = (source_position - source_frame as f64) as f32;

        for channel in 0..channels {
            let current = samples[source_frame * channels + channel];
            let next = samples[next_frame * channels + channel];
            output.push(current + (next - current) * fraction);
        }
    }

    output
}

fn invalid_input(message: impl Into<String>) -> AudioError {
    AudioError::InvalidInput(message.into())
}

fn path_extension(path: &Path) -> Option<&str> {
    path.extension().and_then(|extension| extension.to_str())
}

fn normalize_extension(extension: &str) -> String {
    extension.trim_start_matches('.').to_ascii_lowercase()
}

fn output_error(error: impl fmt::Display) -> AudioError {
    AudioError::Output(error.to_string())
}

fn encode_error(error: impl fmt::Display) -> AudioError {
    AudioError::Encode(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::SinkExt;
    use tokio_stream::StreamExt;

    #[test]
    fn audio_stream_not_found_message_is_stable() {
        assert_eq!(
            AudioError::AudioStreamNotFound.to_string(),
            "audio stream not found"
        );
    }

    #[tokio::test]
    async fn decoder_stream_reads_wav_frames() -> Result<()> {
        let input = temp_path("decoder-input.wav")?;

        write_test_wav_file(&input, &[0.0, 0.25, -0.25, 0.0], 2, 48_000)?;

        let mut stream = AudioStream::from_file(input.clone());
        let frame = stream
            .next()
            .await
            .ok_or_else(|| invalid_input("missing decoded frame"))??;

        assert_eq!(frame.channels, 2);
        assert_eq!(frame.sample_rate, 48_000);
        assert_eq!(frame.samples.len(), 4);

        let _ = std::fs::remove_file(input);
        Ok(())
    }

    #[tokio::test]
    async fn output_stream_writes_wav_frames() -> Result<()> {
        let output = temp_path("writer-output.wav")?;
        let frames: Vec<Result<AudioFrame>> = vec![Ok(AudioFrame {
            samples: vec![0.0, 0.25, -0.25, 0.0],
            channels: 2,
            sample_rate: 48_000,
        })];

        let mut output_sink = AudioSink::to_file(output.clone());
        let mut stream = Box::pin(tokio_stream::iter(frames));

        while let Some(frame) = stream.next().await {
            output_sink.send(frame?).await?;
        }

        output_sink.close().await?;

        let mut stream = AudioStream::from_file(output.clone());
        let frame = stream
            .next()
            .await
            .ok_or_else(|| invalid_input("missing decoded frame"))??;

        assert_eq!(frame.channels, 2);
        assert_eq!(frame.sample_rate, 48_000);
        assert_eq!(frame.samples.len(), 4);

        let _ = std::fs::remove_file(output);
        Ok(())
    }

    #[tokio::test]
    async fn convert_wav_to_wav() -> Result<()> {
        let input = temp_path("convert-input.wav")?;
        let output = temp_path("convert-output.wav")?;

        write_test_wav_file(&input, &[0.0, 0.25, -0.25, 0.0], 2, 48_000)?;
        let mut input_stream = AudioStream::from_file(input.clone());
        let mut output_sink = AudioSink::to_file(output.clone());

        while let Some(frame) = input_stream.next().await {
            output_sink.send(frame?).await?;
        }

        output_sink.close().await?;

        let mut stream = AudioStream::from_file(output.clone());
        let converted = stream
            .next()
            .await
            .ok_or_else(|| invalid_input("missing decoded frame"))??;

        assert_eq!(converted.channels, 2);
        assert_eq!(converted.sample_rate, 48_000);
        assert_eq!(converted.samples.len(), 4);

        let _ = std::fs::remove_file(input);
        let _ = std::fs::remove_file(output);
        Ok(())
    }

    #[test]
    fn resample_linear_preserves_duration_when_upsampling() {
        let input = vec![0.0_f32, 0.5, 1.0, 0.5, 0.0, -0.5, -1.0, -0.5];
        let input_frames = input.len();

        let output = resample_linear(&input, 1, 8000, 48000);

        let expected_frames = input_frames * 48000 / 8000;
        assert_eq!(output.len(), expected_frames);
    }

    #[test]
    fn resample_linear_preserves_duration_when_downsampling() {
        let input = vec![0.0_f32; 480];
        let input_frames = input.len();

        let output = resample_linear(&input, 1, 48000, 8000);

        let expected_frames = input_frames * 8000 / 48000;
        assert_eq!(output.len(), expected_frames);
    }

    #[test]
    fn resample_linear_identity_when_rates_match() {
        let input = vec![0.0_f32, 0.25, -0.25, 0.0];

        let output = resample_linear(&input, 1, 44100, 44100);

        assert_eq!(output.len(), input.len());
        assert_eq!(output, input);
    }

    #[test]
    fn resample_linear_stereo_output_count() {
        let samples_per_channel = 100;
        let input: Vec<f32> = (0..samples_per_channel * 2)
            .map(|i| i as f32 / 200.0)
            .collect();

        let output = resample_linear(&input, 2, 44100, 48000);

        let expected_frames = samples_per_channel * 48000 / 44100;
        assert_eq!(output.len(), expected_frames * 2);
    }

    #[test]
    fn resample_linear_empty_input() {
        let output = resample_linear(&[], 1, 44100, 48000);
        assert!(output.is_empty());
    }

    fn temp_path(name: &str) -> Result<PathBuf> {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| invalid_input(error.to_string()))?
            .as_nanos();

        Ok(std::env::temp_dir().join(format!("airs-audio-{unique}-{name}")))
    }

    #[tokio::test]
    async fn builder_input_reads_wav() -> Result<()> {
        let input = temp_path("builder-input.wav")?;

        write_test_wav_file(&input, &[0.0, 0.25, -0.25, 0.0], 2, 48_000)?;

        let mut stream = AudioStream::from_file(input.clone());
        let frame = stream
            .next()
            .await
            .ok_or_else(|| invalid_input("missing decoded frame"))??;

        assert_eq!(frame.channels, 2);
        assert_eq!(frame.sample_rate, 48_000);
        assert_eq!(frame.samples.len(), 4);

        let _ = std::fs::remove_file(input);
        Ok(())
    }

    #[tokio::test]
    async fn builder_output_writes_wav() -> Result<()> {
        let output = temp_path("builder-output.wav")?;
        let frames: Vec<Result<AudioFrame>> = vec![Ok(AudioFrame {
            samples: vec![0.0, 0.25, -0.25, 0.0],
            channels: 2,
            sample_rate: 48_000,
        })];

        let mut sink = AudioSink::to_file(output.clone());
        let mut stream = Box::pin(tokio_stream::iter(frames));

        while let Some(frame) = stream.next().await {
            sink.send(frame?).await?;
        }

        sink.close().await?;

        let mut stream = AudioStream::from_file(output.clone());
        let frame = stream
            .next()
            .await
            .ok_or_else(|| invalid_input("missing decoded frame"))??;

        assert_eq!(frame.channels, 2);
        assert_eq!(frame.sample_rate, 48_000);
        assert_eq!(frame.samples.len(), 4);

        let _ = std::fs::remove_file(output);
        Ok(())
    }

    #[tokio::test]
    async fn builder_convert_wav_to_wav() -> Result<()> {
        let input = temp_path("builder-convert-input.wav")?;
        let output = temp_path("builder-convert-output.wav")?;

        write_test_wav_file(&input, &[0.0, 0.25, -0.25, 0.0], 2, 48_000)?;
        let mut input_stream = AudioStream::from_file(input.clone());
        let mut output_sink = AudioSink::to_file(output.clone());

        while let Some(frame) = input_stream.next().await {
            output_sink.send(frame?).await?;
        }

        output_sink.close().await?;

        let mut stream = AudioStream::from_file(output.clone());
        let converted = stream
            .next()
            .await
            .ok_or_else(|| invalid_input("missing decoded frame"))??;

        assert_eq!(converted.channels, 2);
        assert_eq!(converted.sample_rate, 48_000);
        assert_eq!(converted.samples.len(), 4);

        let _ = std::fs::remove_file(input);
        let _ = std::fs::remove_file(output);
        Ok(())
    }

    fn write_test_wav_file(
        output: &Path,
        samples: &[f32],
        channels: u16,
        sample_rate: u32,
    ) -> Result<()> {
        let spec = hound::WavSpec {
            channels,
            sample_rate,
            bits_per_sample: 32,
            sample_format: hound::SampleFormat::Float,
        };
        let mut writer = hound::WavWriter::create(output, spec).map_err(output_error)?;

        for sample in samples {
            writer.write_sample(*sample).map_err(output_error)?;
        }

        writer.finalize().map_err(output_error)?;
        Ok(())
    }
}
