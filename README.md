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

- `InputSource` - Re-export from `airs-io`; input source enum shared by text/audio/ASR/TTS crates.
- `OutputTarget` - Re-export from `airs-io`; output target enum shared by text/audio/ASR/TTS crates.

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

- `AudioInput` - Audio input object. Implements `Stream<Item = Result<AudioSlice>>` for file or device input.
- `AudioInput::new(source)` - Select an `InputSource::File` or `InputSource::Device` source.
- `.decoder(decoder)` - Manually set the file decoder and ignore filename inference.
- `.sample_rate(rate)` - Override the sample rate for device streams.
- `.channels(n)` - Override the channel count for device streams.
- `.buffer_size(size)` - Override the buffer size for device streams.
- `AudioStream` - Alias for `AudioInput`.

- `AudioOutput` - Audio output object. Implements `Sink<AudioSlice, Error = AudioError>` for file or device output.
- `AudioOutput::new(target)` - Select an `OutputTarget::File` or `OutputTarget::Device` target.
- `.encoder(encoder)` - Manually set the file encoder and ignore filename inference.
- `.sample_rate(rate)` - Override the sample rate for device streams or file output.
- `.channels(n)` - Override the channel count for device streams or file output.
- `.buffer_size(size)` - Override the buffer size for device streams.
- `AudioSink` - Alias for `AudioOutput`.