---
name: airs-audio
description: Pipe audio between devices and files.
---

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

Input formats: AAC, FLAC, MP3, MP4/M4A, Ogg Vorbis, PCM, WAV. Output formats: WAV, Opus. Formats are selected by file extension.
