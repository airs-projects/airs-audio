# airs-audio

This document lists the CLI and public API.

## CLI

- `airs-audio --help` - Show help.
- `airs-audio --version` - Show version.
- `airs-audio list_devices` - List audio devices.
- `airs-audio pipe -i:d [device] -i:f <file> -o:d [device] -o:f <file>` - Pipe audio.

`-i:d` or `-i:f` is required once. `-o:d` or `-o:f` is required at least once and may be repeated.

```
airs-audio pipe -i:f music.wav -o:d
airs-audio pipe -i:d -o:f record.wav
airs-audio pipe -i:d mic -o:d speaker -o:f record.wav
```

## Library public API

- `version()` - Return the crate version string.
- `Result<T>` - Library result type using `AudioError`.
- `AudioError` - Error enum for device, stream, decode, input, and I/O failures.

- `list_audio_devices()` - List input and output devices with default markers.
- `AudioDeviceList` - Input and output device info lists.
- `AudioDeviceInfo` - Device name plus default-device marker.

- `AudioType` - Public audio type enum for supported format selection.
- `AudioType::from_extension(extension)` - Infer an audio type from an extension.
- `AudioEncoder` - Output encoder selection for file output streams.
- `AudioEncoder::from_type(audio_type)` - Create output encoder selection from an `AudioType`.
- `AudioDecoder` - Audio decoder selection for file input streams.
- `AudioDecoder::from_type(audio_type)` - Create decoder selection from an `AudioType`.
- `AudioSlice` - Interleaved `f32` PCM samples with channel count and sample rate.

- `AudioInput` - Self-builder for device or file input. Consumed by `.open()` to produce an `AudioStream`.
- `AudioInput::default_device()` - Select the default input device.
- `AudioInput::device(name)` - Select an input device by name.
- `AudioInput::file(input)` - Select file input and infer decoder from filename.
- `.decoder(decoder)` - Manually set the file decoder and ignore filename inference.
- `.sample_rate(rate)` - Override the sample rate for device streams.
- `.channels(n)` - Override the channel count for device streams.
- `.buffer_size(size)` - Override the buffer size for device streams.
- `.open(self)` - Consume and build the `AudioStream`.
- `AudioStream` - Boxed Tokio stream of decoded or captured `AudioSlice` values.

- `AudioOutput` - Self-builder for file or device output. Consumed by `.open()` to produce an `AudioSink`.
- `AudioOutput::default_device()` - Select the default output device.
- `AudioOutput::device(name)` - Select an output device by name.
- `AudioOutput::file(output)` - Select file output and infer encoder from filename.
- `.encoder(encoder)` - Manually set the file encoder and ignore filename inference.
- `.sample_rate(rate)` - Override the sample rate for device streams or file output.
- `.channels(n)` - Override the channel count for device streams or file output.
- `.buffer_size(size)` - Override the buffer size for device streams.
- `.open(self)` - Consume and build the `AudioSink`.
- `AudioSink` - `Pin<Box<dyn Sink<AudioSlice, Error = AudioError> + Send>>` for output to files or devices.
