# airs-audio

This document lists the CLI and public API.

## CLI

- `airs-audio --help` - Show help.
- `airs-audio --version` - Show version.
- `airs-audio list_devices` - List audio devices.
- `airs-audio pipe -i <source> -o <target> [-o <target>...]` - Pipe audio.

`-i` is required once. `-o` is required at least once and may be repeated.

Sources and targets:
- `file:<path>`
- `device`
- `device:<name>`

```
airs-audio pipe -i file:music.wav -o device
airs-audio pipe -i device -o file:record.wav
airs-audio pipe -i device:mic -o device:speaker -o file:record.wav
```

## Library public API

- `version()` - Return the crate version string.
- `Result<T>` - Library result type using `AudioError`.
- `AudioError` - Error enum for device, stream, decode, input, and I/O failures.

- `list_audio_devices()` - List input and output devices with default markers.
- `AudioDeviceList` - Input and output device info lists.
- `AudioDeviceInfo` - Device name plus default-device marker.

- `AudioFrame` - Interleaved `f32` PCM samples with channel count and sample rate.

- `AudioStream` - Audio input stream. Implements `Stream<Item = Result<AudioFrame>>` for file or device input.
- `AudioStream::from_file(path)` - Read audio from a file.
- `AudioStream::from_device(name)` - Read audio from the default input device or a named input device.
- `.sample_rate(rate)` - Override the sample rate for device streams.
- `.channels(n)` - Override the channel count for device streams.
- `.buffer_size(size)` - Override the buffer size for device streams.

- `AudioSink` - Audio output sink. Implements `Sink<AudioFrame, Error = AudioError>` for file or device output.
- `AudioSink::to_file(path)` - Write audio to a file.
- `AudioSink::to_device(name)` - Write audio to the default output device or a named output device.
- `.sample_rate(rate)` - Override the sample rate for device streams or file output.
- `.channels(n)` - Override the channel count for device streams or file output.
- `.buffer_size(size)` - Override the buffer size for device streams.
