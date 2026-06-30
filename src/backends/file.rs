use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

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
use tokio_stream::Stream;

use crate::*;

const OPUS_SAMPLE_RATE: u32 = 48_000;
const OPUS_FRAME_SAMPLES: usize = 960;
const OPUS_MAX_PACKET_BYTES: usize = 4_000;
const OGG_SERIAL: u32 = 0x4149_5253;

pub(crate) fn stream(path: &Path) -> Result<BoxedAudioStream> {
    let file = File::open(path)?;

    Ok(Box::pin(FileAudioStream::open(file, path)?))
}

pub(crate) fn sink(
    path: &Path,
    sample_rate: Option<u32>,
    channels: Option<u16>,
) -> Result<BoxedAudioSink> {
    let writer = match path_extension(path).map(normalize_extension).as_deref() {
        Some("wav") => FileStreamWriterKind::Wav,
        Some("opus") => FileStreamWriterKind::Opus,
        Some(_) => return Err(invalid_input("output file must be wav or opus")),
        None => return Err(invalid_input("output file must have an extension")),
    };

    Ok(Box::pin(FileAudioSink {
        output: path.to_path_buf(),
        writer,
        sample_rate,
        channels,
        state: None,
    }))
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
    fn open(file: File, path: &Path) -> Result<Self> {
        let media_source = MediaSourceStream::new(Box::new(file), Default::default());
        let mut hint = Hint::new();
        if let Some(extension) = path_extension(path) {
            hint.with_extension(extension);
        }

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

    fn next_frame(&mut self) -> Option<Result<AudioFrame>> {
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

            return Some(Ok(AudioFrame {
                samples: sample_buffer.samples().to_vec(),
                channels,
                sample_rate,
            }));
        }
    }
}

impl Stream for FileAudioStream {
    type Item = Result<AudioFrame>;

    fn poll_next(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.next_frame())
    }
}

struct FileAudioSink {
    output: PathBuf,
    writer: FileStreamWriterKind,
    sample_rate: Option<u32>,
    channels: Option<u16>,
    state: Option<FileSinkState>,
}

struct FileSinkState {
    writer: FileStreamWriter,
    channels: u16,
    sample_rate: u32,
}

enum FileStreamWriter {
    Wav(WavStreamWriter),
    Opus(OpusStreamWriter),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileStreamWriterKind {
    Wav,
    Opus,
}

impl Unpin for FileAudioSink {}

impl FileAudioSink {
    fn send_frame(&mut self, frame: AudioFrame) -> Result<()> {
        let mut frame = frame;
        if let Some(sample_rate) = self.sample_rate {
            frame.sample_rate = sample_rate;
        }
        if let Some(channels) = self.channels {
            frame.channels = channels;
        }
        validate_frame(&frame)?;

        if self.state.is_none() {
            let writer = match self.writer {
                FileStreamWriterKind::Wav => {
                    FileStreamWriter::Wav(WavStreamWriter::create(&self.output, &frame)?)
                }
                FileStreamWriterKind::Opus => {
                    FileStreamWriter::Opus(OpusStreamWriter::create(&self.output, &frame, None)?)
                }
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

impl FileStreamWriter {
    fn write_frame(&mut self, frame: &AudioFrame) -> Result<()> {
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

impl Sink<AudioFrame> for FileAudioSink {
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

    fn poll_close(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(self.close_state())
    }
}

struct WavStreamWriter {
    writer: hound::WavWriter<BufWriter<File>>,
}

impl WavStreamWriter {
    fn create(output: &Path, frame: &AudioFrame) -> Result<Self> {
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

    fn write_frame(&mut self, frame: &AudioFrame) -> Result<()> {
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
    fn create(output: &Path, frame: &AudioFrame, bitrate: Option<i32>) -> Result<Self> {
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

    fn write_frame(&mut self, frame: &AudioFrame) -> Result<()> {
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

fn opus_head(channels: u8, pre_skip: u16) -> Vec<u8> {
    let mut data = Vec::with_capacity(19);
    data.extend_from_slice(b"OpusHead");
    data.push(1);
    data.push(channels);
    data.extend_from_slice(&pre_skip.to_le_bytes());
    data.extend_from_slice(&OPUS_SAMPLE_RATE.to_le_bytes());
    data.extend_from_slice(&0_i16.to_le_bytes());
    data.push(0);
    data
}

fn opus_tags() -> Vec<u8> {
    let vendor = b"airs-audio";
    let mut data = Vec::with_capacity(8 + 4 + vendor.len() + 4);
    data.extend_from_slice(b"OpusTags");
    data.extend_from_slice(&(vendor.len() as u32).to_le_bytes());
    data.extend_from_slice(vendor);
    data.extend_from_slice(&0_u32.to_le_bytes());
    data
}

impl From<SymphoniaError> for AudioError {
    fn from(error: SymphoniaError) -> Self {
        Self::Decode(error.to_string())
    }
}
