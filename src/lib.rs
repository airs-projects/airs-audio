use std::collections::VecDeque;
use std::fmt;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use futures::Sink;
use ogg::{PacketWriteEndInfo, PacketWriter};
use opus2::{Application, Bitrate, Channels, Encoder};
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::{CODEC_TYPE_NULL, DecoderOptions};
use symphonia::core::errors::Error as SymphoniaError;
use symphonia::core::formats::{FormatOptions, FormatReader};
use symphonia::core::io::MediaSourceStream;
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::UnboundedReceiverStream;

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub type Result<T> = std::result::Result<T, AudioError>;
pub type AudioStream = Pin<Box<dyn Stream<Item = Result<AudioSlice>> + Send>>;
pub type AudioSink = Pin<Box<dyn Sink<AudioSlice, Error = AudioError> + Send>>;

const OPUS_SAMPLE_RATE: u32 = 48_000;
const OPUS_FRAME_SAMPLES: usize = 960;
const OPUS_MAX_PACKET_BYTES: usize = 4_000;
const OGG_SERIAL: u32 = 0x4149_5253;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AudioType {
    Aac,
    Flac,
    Mp3,
    Opus,
    Vorbis,
    Wav,
}

impl AudioType {
    pub fn from_extension(extension: impl AsRef<str>) -> Result<Self> {
        match normalize_extension(extension.as_ref()).as_str() {
            "aac" => Ok(Self::Aac),
            "flac" => Ok(Self::Flac),
            "mp3" => Ok(Self::Mp3),
            "opus" => Ok(Self::Opus),
            "ogg" | "vorbis" => Ok(Self::Vorbis),
            "wav" => Ok(Self::Wav),
            _ => Err(invalid_input("unsupported audio extension")),
        }
    }

    fn extension(self) -> &'static str {
        match self {
            Self::Aac => "aac",
            Self::Flac => "flac",
            Self::Mp3 => "mp3",
            Self::Opus => "opus",
            Self::Vorbis => "ogg",
            Self::Wav => "wav",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AudioEncoder {
    audio_type: AudioType,
}

impl AudioEncoder {
    pub fn from_type(audio_type: AudioType) -> Result<Self> {
        match audio_type {
            AudioType::Opus | AudioType::Wav => Ok(Self { audio_type }),
            _ => Err(invalid_input("output type must be opus or wav")),
        }
    }

    fn audio_type(&self) -> AudioType {
        self.audio_type
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AudioDecoder {
    audio_type: Option<AudioType>,
}

impl AudioDecoder {
    pub fn from_type(audio_type: AudioType) -> Self {
        Self {
            audio_type: Some(audio_type),
        }
    }

    fn hint(&self) -> Hint {
        let mut hint = Hint::new();

        if let Some(audio_type) = self.audio_type {
            hint.with_extension(audio_type.extension());
        }

        hint
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AudioSlice {
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

pub struct AudioDeviceInfo {
    pub name: String,
    pub is_default: bool,
}

pub struct AudioDeviceList {
    pub inputs: Vec<AudioDeviceInfo>,
    pub outputs: Vec<AudioDeviceInfo>,
}

pub fn list_audio_devices() -> Result<AudioDeviceList> {
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

struct FileAudioStream {
    format: Box<dyn FormatReader>,
    decoder: Box<dyn symphonia::core::codecs::Decoder>,
    track_id: u32,
    sample_rate: Option<u32>,
    channels: Option<usize>,
    done: bool,
}

impl FileAudioStream {
    fn open(file: File, decoder: &AudioDecoder) -> Result<Self> {
        let media_source = MediaSourceStream::new(Box::new(file), Default::default());
        let hint = decoder.hint();

        let probed = symphonia::default::get_probe().format(
            &hint,
            media_source,
            &FormatOptions::default(),
            &MetadataOptions::default(),
        )?;
        let format = probed.format;

        let (track_id, codec_params) = {
            let track = format
                .tracks()
                .iter()
                .find(|track| track.codec_params.codec != CODEC_TYPE_NULL)
                .ok_or(AudioError::AudioStreamNotFound)?;

            (track.id, track.codec_params.clone())
        };

        let decoder =
            symphonia::default::get_codecs().make(&codec_params, &DecoderOptions::default())?;

        Ok(Self {
            format,
            decoder,
            track_id,
            sample_rate: codec_params.sample_rate,
            channels: codec_params.channels.map(|channels| channels.count()),
            done: false,
        })
    }

    fn next_frame(&mut self) -> Option<Result<AudioSlice>> {
        if self.done {
            return None;
        }

        loop {
            let packet = match self.format.next_packet() {
                Ok(packet) => packet,
                Err(SymphoniaError::IoError(error))
                    if error.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    self.done = true;
                    return None;
                }
                Err(error) => {
                    self.done = true;
                    return Some(Err(error.into()));
                }
            };

            if packet.track_id() != self.track_id {
                continue;
            }

            let decoded = match self.decoder.decode(&packet) {
                Ok(decoded) => decoded,
                Err(SymphoniaError::DecodeError(_)) => continue,
                Err(error) => {
                    self.done = true;
                    return Some(Err(error.into()));
                }
            };
            let spec = *decoded.spec();
            let sample_rate = self.sample_rate.or(Some(spec.rate)).unwrap_or_default();
            let channel_count = self
                .channels
                .or(Some(spec.channels.count()))
                .unwrap_or_default();

            self.sample_rate = Some(sample_rate);
            self.channels = Some(channel_count);

            let channels = match u16::try_from(channel_count) {
                Ok(channels) => channels,
                Err(_) => return Some(Err(invalid_input("too many audio channels"))),
            };

            let mut sample_buffer = SampleBuffer::<f32>::new(decoded.capacity() as u64, spec);
            sample_buffer.copy_interleaved_ref(decoded);

            return Some(Ok(AudioSlice {
                samples: sample_buffer.samples().to_vec(),
                channels,
                sample_rate,
            }));
        }
    }
}

impl Stream for FileAudioStream {
    type Item = Result<AudioSlice>;

    fn poll_next(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.next_frame())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AudioInputSource {
    Device(Option<String>),
    File(PathBuf),
}

pub struct AudioInput {
    source: AudioInputSource,
    decoder: Option<AudioDecoder>,
    sample_rate: Option<u32>,
    channels: Option<u16>,
    buffer_size: Option<u32>,
}

struct DeviceAudioStream {
    _stream: cpal::Stream,
    frames: UnboundedReceiverStream<Result<AudioSlice>>,
}

impl AudioInput {
    pub fn new(source: AudioInputSource) -> Self {
        Self {
            source,
            decoder: None,
            sample_rate: None,
            channels: None,
            buffer_size: None,
        }
    }

    pub fn decoder(mut self, decoder: AudioDecoder) -> Self {
        self.decoder = Some(decoder);
        self
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

    pub fn open(self) -> Result<AudioStream> {
        match self.source {
            AudioInputSource::Device(device_name) => {
                let input = match device_name {
                    Some(device_name) => InputDevice::named(device_name)?,
                    None => InputDevice::default()?,
                };
                let config = input.device.default_input_config()?;
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

                stream.play()?;

                Ok(Box::pin(DeviceAudioStream {
                    _stream: stream,
                    frames: UnboundedReceiverStream::new(receiver),
                }))
            }
            AudioInputSource::File(input) => {
                let decoder = match self.decoder {
                    Some(decoder) => decoder,
                    None => match path_extension(&input) {
                        Some(extension) => {
                            AudioDecoder::from_type(AudioType::from_extension(extension)?)
                        }
                        None => AudioDecoder { audio_type: None },
                    },
                };
                let file = File::open(&input)?;

                Ok(Box::pin(FileAudioStream::open(file, &decoder)?))
            }
        }
    }
}

impl Unpin for DeviceAudioStream {}

impl Stream for DeviceAudioStream {
    type Item = Result<AudioSlice>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.frames).poll_next(context)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AudioOutputTarget {
    Device(Option<String>),
    File(PathBuf),
}

pub struct AudioOutput {
    target: AudioOutputTarget,
    encoder: Option<AudioEncoder>,
    sample_rate: Option<u32>,
    channels: Option<u16>,
    buffer_size: Option<u32>,
}

struct FileAudioSink {
    output: PathBuf,
    encoder: AudioEncoder,
    sample_rate: Option<u32>,
    channels: Option<u16>,
    state: Option<FileSinkState>,
}

struct FileSinkState {
    writer: FileStreamWriter,
    channels: u16,
    sample_rate: u32,
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

enum FileStreamWriter {
    Wav(WavStreamWriter),
    Opus(OpusStreamWriter),
}

impl Unpin for FileAudioSink {}
impl Unpin for DeviceAudioSink {}

impl AudioOutput {
    pub fn new(target: AudioOutputTarget) -> Self {
        Self {
            target,
            encoder: None,
            sample_rate: None,
            channels: None,
            buffer_size: None,
        }
    }

    pub fn encoder(mut self, encoder: AudioEncoder) -> Self {
        self.encoder = Some(encoder);
        self
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

    pub fn open(self) -> Result<AudioSink> {
        match &self.target {
            AudioOutputTarget::File(output) => {
                let encoder = match self.encoder {
                    Some(encoder) => encoder,
                    None => {
                        let extension = path_extension(output)
                            .ok_or_else(|| invalid_input("output file must have an extension"))?;
                        AudioEncoder::from_type(AudioType::from_extension(extension)?)?
                    }
                };

                Ok(Box::pin(FileAudioSink {
                    output: output.clone(),
                    encoder,
                    sample_rate: self.sample_rate,
                    channels: self.channels,
                    state: None,
                }))
            }
            AudioOutputTarget::Device(device_name) => Ok(Box::pin(DeviceAudioSink {
                device_name: device_name.clone(),
                sample_rate: self.sample_rate,
                channels: self.channels,
                buffer_size: self.buffer_size,
                state: None,
            })),
        }
    }
}

impl FileAudioSink {
    fn send_frame(&mut self, frame: AudioSlice) -> Result<()> {
        let mut frame = frame;
        if let Some(sample_rate) = self.sample_rate {
            frame.sample_rate = sample_rate;
        }
        if let Some(channels) = self.channels {
            frame.channels = channels;
        }
        validate_frame(&frame)?;

        if self.state.is_none() {
            let writer = match self.encoder.audio_type() {
                AudioType::Wav => {
                    FileStreamWriter::Wav(WavStreamWriter::create(&self.output, &frame)?)
                }
                AudioType::Opus => {
                    FileStreamWriter::Opus(OpusStreamWriter::create(&self.output, &frame, None)?)
                }
                _ => unreachable!("audio encoder only supports wav or opus"),
            };
            self.state = Some(FileSinkState {
                writer,
                channels: frame.channels,
                sample_rate: frame.sample_rate,
            });
        } else {
            let state = self
                .state
                .as_ref()
                .expect("file sink state initialized above");
            validate_frame_format(state.channels, state.sample_rate, &frame)?;
        }

        let state = self
            .state
            .as_mut()
            .expect("file sink state initialized above");
        state.writer.write_frame(&frame)
    }

    fn close_state(&mut self) -> Result<()> {
        if let Some(state) = self.state.take() {
            state.writer.finalize()?;
        }
        Ok(())
    }
}

impl DeviceAudioSink {
    fn send_frame(&mut self, frame: AudioSlice) -> Result<()> {
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

            // Queue the first frame BEFORE starting the stream so the
            // WASAPI callback never sees an empty buffer on init.
            enqueue_output_frame(
                &playback,
                &frame,
                output_channels,
                output_sample_rate,
            )?;

            // Start playback after samples are ready.
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

impl FileStreamWriter {
    fn write_frame(&mut self, frame: &AudioSlice) -> Result<()> {
        match self {
            Self::Wav(writer) => writer.write_frame(frame),
            Self::Opus(writer) => writer.write_frame(frame),
        }
    }

    fn finalize(self) -> Result<()> {
        match self {
            Self::Wav(writer) => writer.finalize(),
            Self::Opus(writer) => writer.finalize(),
        }
    }
}

impl Sink<AudioSlice> for FileAudioSink {
    type Error = AudioError;

    fn poll_ready(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: AudioSlice) -> Result<()> {
        self.send_frame(item)
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(self.close_state())
    }
}

impl Sink<Result<AudioSlice>> for FileAudioSink {
    type Error = AudioError;

    fn poll_ready(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<()>> {
        <Self as Sink<AudioSlice>>::poll_ready(self, context)
    }

    fn start_send(self: Pin<&mut Self>, item: Result<AudioSlice>) -> Result<()> {
        <Self as Sink<AudioSlice>>::start_send(self, item?)
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<()>> {
        <Self as Sink<AudioSlice>>::poll_flush(self, context)
    }

    fn poll_close(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<()>> {
        <Self as Sink<AudioSlice>>::poll_close(self, context)
    }
}

impl Sink<AudioSlice> for DeviceAudioSink {
    type Error = AudioError;

    fn poll_ready(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: AudioSlice) -> Result<()> {
        self.send_frame(item)
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<()>> {
        if let Some(state) = &self.state {
            if !playback_is_empty(&state.playback, Some(context.waker().clone())) {
                return Poll::Pending;
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl Sink<Result<AudioSlice>> for DeviceAudioSink {
    type Error = AudioError;

    fn poll_ready(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<()>> {
        <Self as Sink<AudioSlice>>::poll_ready(self, context)
    }

    fn start_send(self: Pin<&mut Self>, item: Result<AudioSlice>) -> Result<()> {
        <Self as Sink<AudioSlice>>::start_send(self, item?)
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<()>> {
        <Self as Sink<AudioSlice>>::poll_flush(self, context)
    }

    fn poll_close(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<()>> {
        <Self as Sink<AudioSlice>>::poll_close(self, context)
    }
}

struct WavStreamWriter {
    writer: hound::WavWriter<BufWriter<File>>,
}

impl WavStreamWriter {
    fn create(output: &Path, frame: &AudioSlice) -> Result<Self> {
        let spec = hound::WavSpec {
            channels: frame.channels,
            sample_rate: frame.sample_rate,
            bits_per_sample: 32,
            sample_format: hound::SampleFormat::Float,
        };

        Ok(Self {
            writer: hound::WavWriter::create(output, spec).map_err(output_error)?,
        })
    }

    fn write_frame(&mut self, frame: &AudioSlice) -> Result<()> {
        for sample in &frame.samples {
            self.writer.write_sample(*sample).map_err(output_error)?;
        }

        Ok(())
    }

    fn finalize(self) -> Result<()> {
        self.writer.finalize().map_err(output_error)?;
        Ok(())
    }
}

struct OpusStreamWriter {
    writer: PacketWriter<'static, BufWriter<File>>,
    encoder: Encoder,
    output_channels: usize,
    pre_skip: u64,
    pending: Vec<f32>,
    total_samples: u64,
}

impl OpusStreamWriter {
    fn create(output: &Path, frame: &AudioSlice, bitrate: Option<i32>) -> Result<Self> {
        let output_channels = if frame.channels == 1 { 1 } else { 2 };
        let opus_channels = match output_channels {
            1 => Channels::Mono,
            2 => Channels::Stereo,
            _ => unreachable!("opus output channel count is limited above"),
        };
        let mut encoder = Encoder::new(OPUS_SAMPLE_RATE, opus_channels, Application::Audio)
            .map_err(encode_error)?;

        if let Some(bitrate) = bitrate {
            encoder
                .set_bitrate(Bitrate::Bits(bitrate))
                .map_err(encode_error)?;
        }

        let pre_skip: u16 = encoder.get_lookahead().map_err(encode_error)?.try_into()?;
        let file = BufWriter::new(File::create(output)?);
        let mut writer = PacketWriter::new(file);

        writer
            .write_packet(
                opus_head(output_channels as u8, pre_skip),
                OGG_SERIAL,
                PacketWriteEndInfo::EndPage,
                0,
            )
            .map_err(output_error)?;
        writer
            .write_packet(opus_tags(), OGG_SERIAL, PacketWriteEndInfo::EndPage, 0)
            .map_err(output_error)?;

        Ok(Self {
            writer,
            encoder,
            output_channels,
            pre_skip: pre_skip as u64,
            pending: Vec::new(),
            total_samples: 0,
        })
    }

    fn write_frame(&mut self, frame: &AudioSlice) -> Result<()> {
        let opus_samples = prepare_opus_samples(
            &frame.samples,
            frame.channels as usize,
            frame.sample_rate,
            self.output_channels,
        );

        self.pending.extend_from_slice(&opus_samples);
        self.write_ready_packets()
    }

    fn finalize(mut self) -> Result<()> {
        let frame_len = OPUS_FRAME_SAMPLES * self.output_channels;
        let copied = self.pending.len();
        let mut frame = vec![0.0; frame_len];

        frame[..copied].copy_from_slice(&self.pending);
        self.total_samples += (copied / self.output_channels) as u64;

        let packet = self
            .encoder
            .encode_vec_float(&frame, OPUS_MAX_PACKET_BYTES)
            .map_err(encode_error)?;
        self.writer
            .write_packet(
                packet,
                OGG_SERIAL,
                PacketWriteEndInfo::EndStream,
                self.pre_skip + self.total_samples,
            )
            .map_err(output_error)?;

        let mut file = self.writer.into_inner();
        file.flush()?;
        Ok(())
    }

    fn write_ready_packets(&mut self) -> Result<()> {
        let frame_len = OPUS_FRAME_SAMPLES * self.output_channels;

        while self.pending.len() > frame_len {
            let frame: Vec<f32> = self.pending.drain(..frame_len).collect();
            let packet = self
                .encoder
                .encode_vec_float(&frame, OPUS_MAX_PACKET_BYTES)
                .map_err(encode_error)?;

            self.total_samples += OPUS_FRAME_SAMPLES as u64;
            self.writer
                .write_packet(
                    packet,
                    OGG_SERIAL,
                    PacketWriteEndInfo::NormalPacket,
                    self.pre_skip + self.total_samples,
                )
                .map_err(output_error)?;
        }

        Ok(())
    }
}

struct PlaybackState {
    samples: VecDeque<f32>,
    waker: Option<Waker>,
}

fn enqueue_output_frame(
    state: &Arc<Mutex<PlaybackState>>,
    frame: &AudioSlice,
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
    sender: mpsc::UnboundedSender<Result<AudioSlice>>,
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
                    let frame = AudioSlice {
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

fn validate_frame(frame: &AudioSlice) -> Result<()> {
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

fn validate_frame_format(channels: u16, sample_rate: u32, frame: &AudioSlice) -> Result<()> {
    if frame.channels != channels || frame.sample_rate != sample_rate {
        return Err(invalid_input("audio slice format changed"));
    }

    Ok(())
}

fn prepare_opus_samples(
    samples: &[f32],
    input_channels: usize,
    input_sample_rate: u32,
    output_channels: usize,
) -> Vec<f32> {
    let remixed = remix_channels(samples, input_channels, output_channels);

    if input_sample_rate == OPUS_SAMPLE_RATE {
        remixed
    } else {
        resample_linear(
            &remixed,
            output_channels,
            input_sample_rate,
            OPUS_SAMPLE_RATE,
        )
    }
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

fn opus_head(channels: u8, pre_skip: u16) -> Vec<u8> {
    let mut packet = Vec::with_capacity(19);

    packet.extend_from_slice(b"OpusHead");
    packet.push(1);
    packet.push(channels);
    packet.extend_from_slice(&pre_skip.to_le_bytes());
    packet.extend_from_slice(&OPUS_SAMPLE_RATE.to_le_bytes());
    packet.extend_from_slice(&0i16.to_le_bytes());
    packet.push(0);

    packet
}

fn opus_tags() -> Vec<u8> {
    let vendor = b"airs-audio";
    let mut packet = Vec::new();

    packet.extend_from_slice(b"OpusTags");
    packet.extend_from_slice(&(vendor.len() as u32).to_le_bytes());
    packet.extend_from_slice(vendor);
    packet.extend_from_slice(&0u32.to_le_bytes());

    packet
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

        let mut stream = AudioInput::new(AudioInputSource::File(input.clone())).open()?;
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
        let frames: Vec<Result<AudioSlice>> = vec![Ok(AudioSlice {
            samples: vec![0.0, 0.25, -0.25, 0.0],
            channels: 2,
            sample_rate: 48_000,
        })];

        let mut output_sink = AudioOutput::new(AudioOutputTarget::File(output.clone())).open()?;
        let mut stream = Box::pin(tokio_stream::iter(frames));

        while let Some(frame) = stream.next().await {
            output_sink.send(frame?).await?;
        }

        output_sink.close().await?;

        let mut stream = AudioInput::new(AudioInputSource::File(output.clone())).open()?;
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
        let mut input_stream = AudioInput::new(AudioInputSource::File(input.clone())).open()?;
        let mut output_sink = AudioOutput::new(AudioOutputTarget::File(output.clone())).open()?;

        while let Some(frame) = input_stream.next().await {
            output_sink.send(frame?).await?;
        }

        output_sink.close().await?;

        let mut stream = AudioInput::new(AudioInputSource::File(output.clone())).open()?;
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

        let mut stream = AudioInput::new(AudioInputSource::File(input.clone())).open()?;
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
        let frames: Vec<Result<AudioSlice>> = vec![Ok(AudioSlice {
            samples: vec![0.0, 0.25, -0.25, 0.0],
            channels: 2,
            sample_rate: 48_000,
        })];

        let mut sink = AudioOutput::new(AudioOutputTarget::File(output.clone())).open()?;
        let mut stream = Box::pin(tokio_stream::iter(frames));

        while let Some(frame) = stream.next().await {
            sink.send(frame?).await?;
        }

        sink.close().await?;

        let mut stream = AudioInput::new(AudioInputSource::File(output.clone())).open()?;
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
        let mut input_stream = AudioInput::new(AudioInputSource::File(input.clone())).open()?;
        let mut output_sink = AudioOutput::new(AudioOutputTarget::File(output.clone())).open()?;

        while let Some(frame) = input_stream.next().await {
            output_sink.send(frame?).await?;
        }

        output_sink.close().await?;

        let mut stream = AudioInput::new(AudioInputSource::File(output.clone())).open()?;
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
