use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use airs_audio::{AudioSink, AudioStream, list_audio_devices};
use futures::SinkExt;
use tokio_stream::StreamExt;

#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceSpec {
    Device(Option<String>),
    File(PathBuf),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TargetSpec {
    Device(Option<String>),
    File(PathBuf),
}

#[derive(Debug)]
enum Command {
    Help,
    Version,
    Devices,
    Pipe {
        source: SourceSpec,
        targets: Vec<TargetSpec>,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    match parse_command(args)? {
        Command::Help => cmd_help(),
        Command::Version => println!("{}", airs_audio::version()),
        Command::Devices => cmd_list_devices()?,
        Command::Pipe { source, targets } => cmd_pipe(source, targets).await?,
    }

    Ok(())
}

fn parse_command(args: Vec<String>) -> Result<Command, io::Error> {
    match args.as_slice() {
        [] => Ok(Command::Help),
        [arg] if arg == "--help" => Ok(Command::Help),
        [arg] if arg == "--version" => Ok(Command::Version),
        [command] if command == "list_devices" => Ok(Command::Devices),
        [command, ..] if command == "pipe" => parse_pipe(&args[1..]),
        [command, ..] => Err(invalid_input(format!("unknown function: {command}"))),
    }
}

fn parse_pipe(args: &[String]) -> Result<Command, io::Error> {
    let mut source = None;
    let mut targets = Vec::new();
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "-i" => {
                if source.is_some() {
                    return Err(invalid_input("-i can only be used once"));
                }
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| invalid_input("-i requires file:<path> or device[:name]"))?;
                source = Some(parse_source(value)?);
            }
            "-o" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| invalid_input("-o requires file:<path> or device[:name]"))?;
                targets.push(parse_target(value)?);
            }
            arg => return Err(invalid_input(format!("unexpected argument: {arg}"))),
        }
        i += 1;
    }

    let source =
        source.ok_or_else(|| invalid_input("pipe requires -i file:<path> or -i device[:name]"))?;
    if targets.is_empty() {
        return Err(invalid_input(
            "pipe requires at least one -o file:<path> or -o device[:name]",
        ));
    }

    Ok(Command::Pipe { source, targets })
}

fn parse_source(value: &str) -> Result<SourceSpec, io::Error> {
    match split_typed_value(value) {
        ("file", Some(path)) if !path.is_empty() => Ok(SourceSpec::File(PathBuf::from(path))),
        ("file", _) => Err(invalid_input("-i file requires a path")),
        ("device", name) => Ok(SourceSpec::Device(
            name.filter(|name| !name.is_empty()).map(str::to_owned),
        )),
        (kind, _) => Err(invalid_input(format!("unsupported input type: {kind}"))),
    }
}

fn parse_target(value: &str) -> Result<TargetSpec, io::Error> {
    match split_typed_value(value) {
        ("file", Some(path)) if !path.is_empty() => Ok(TargetSpec::File(PathBuf::from(path))),
        ("file", _) => Err(invalid_input("-o file requires a path")),
        ("device", name) => Ok(TargetSpec::Device(
            name.filter(|name| !name.is_empty()).map(str::to_owned),
        )),
        (kind, _) => Err(invalid_input(format!("unsupported output type: {kind}"))),
    }
}

fn split_typed_value(value: &str) -> (&str, Option<&str>) {
    match value.split_once(':') {
        Some((kind, value)) => (kind, Some(value)),
        None => (value, None),
    }
}

fn cmd_help() {
    println!("Usage:");
    println!("  airs-audio --help");
    println!("  airs-audio --version");
    println!("  airs-audio list_devices");
    println!("  airs-audio pipe -i <source> -o <target> [-o <target>...]");
    println!();
    println!("  -i device         Default input device");
    println!("  -i device:<name>  Named input device");
    println!("  -i file:<path>    Input file");
    println!("  -o device         Default output device");
    println!("  -o device:<name>  Named output device");
    println!("  -o file:<path>    Output file");
    println!();
    println!("Examples:");
    println!("  airs-audio pipe -i file:music.wav -o device");
    println!("  airs-audio pipe -i device -o file:record.wav");
    println!("  airs-audio pipe -i device:mic -o device:speaker -o file:record.wav");
}

fn cmd_list_devices() -> Result<(), Box<dyn Error>> {
    let devices = list_audio_devices()?;

    println!("Input devices:");
    for device in devices.inputs {
        print_device(device.name, device.is_default);
    }

    println!();
    println!("Output devices:");
    for device in devices.outputs {
        print_device(device.name, device.is_default);
    }

    Ok(())
}

fn print_device(name: String, is_default: bool) {
    if is_default {
        println!("{name} (default)");
    } else {
        println!("{name}");
    }
}

async fn cmd_pipe(source: SourceSpec, targets: Vec<TargetSpec>) -> Result<(), Box<dyn Error>> {
    let mut input = source.into_stream();
    let mut outputs: Vec<AudioSink> = targets.iter().map(TargetSpec::sink).collect();
    let stop = Arc::new(AtomicBool::new(false));

    let pipeline_stop = stop.clone();
    let pipeline = tokio::spawn(async move {
        input.start()?;

        while !pipeline_stop.load(Ordering::SeqCst) {
            match input.next().await {
                Some(Ok(frame)) => {
                    for output in &mut outputs {
                        output.send(frame.clone()).await?;
                    }
                }
                Some(Err(error)) => return Err(error),
                None => break,
            }
        }

        for output in &mut outputs {
            output.close().await?;
        }

        Ok::<_, airs_audio::AudioError>(())
    });

    eprintln!("Piping audio. Press Ctrl+C to stop.");
    let signal_stop = stop.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        signal_stop.store(true, Ordering::SeqCst);
    });

    pipeline.await??;
    print_written_files(&targets);
    Ok(())
}

impl SourceSpec {
    fn into_stream(self) -> AudioStream {
        match self {
            Self::Device(name) => AudioStream::from_device(name),
            Self::File(path) => AudioStream::from_file(path),
        }
    }
}

impl TargetSpec {
    fn sink(&self) -> AudioSink {
        match self {
            Self::Device(name) => AudioSink::to_device(name.clone()),
            Self::File(path) => AudioSink::to_file(path.clone()),
        }
    }
}

fn print_written_files(targets: &[TargetSpec]) {
    for target in targets {
        if let TargetSpec::File(path) = target {
            eprintln!("Wrote audio file: {}", path.display());
        }
    }
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_list_devices_command() {
        let command = parse_command(vec!["list_devices".to_string()]).expect("parse command");

        match command {
            Command::Devices => {}
            _ => panic!("expected devices command"),
        }
    }

    #[test]
    fn parse_pipe_device_to_device() {
        let command = parse_command(vec![
            "pipe".to_string(),
            "-i".to_string(),
            "device".to_string(),
            "-o".to_string(),
            "device".to_string(),
        ])
        .expect("parse command");

        match command {
            Command::Pipe { source, targets } => {
                assert_eq!(source, SourceSpec::Device(None));
                assert_eq!(targets, vec![TargetSpec::Device(None)]);
            }
            _ => panic!("expected pipe command"),
        }
    }

    #[test]
    fn parse_pipe_device_to_file() {
        let command = parse_command(vec![
            "pipe".to_string(),
            "-i".to_string(),
            "device".to_string(),
            "-o".to_string(),
            "file:record.wav".to_string(),
        ])
        .expect("parse command");

        match command {
            Command::Pipe { source, targets } => {
                assert_eq!(source, SourceSpec::Device(None));
                assert_eq!(targets, vec![TargetSpec::File(PathBuf::from("record.wav"))]);
            }
            _ => panic!("expected pipe command"),
        }
    }

    #[test]
    fn parse_pipe_file_to_device() {
        let command = parse_command(vec![
            "pipe".to_string(),
            "-i".to_string(),
            "file:music.wav".to_string(),
            "-o".to_string(),
            "device".to_string(),
        ])
        .expect("parse command");

        match command {
            Command::Pipe { source, targets } => {
                assert_eq!(source, SourceSpec::File(PathBuf::from("music.wav")));
                assert_eq!(targets, vec![TargetSpec::Device(None)]);
            }
            _ => panic!("expected pipe command"),
        }
    }

    #[test]
    fn parse_pipe_file_to_file() {
        let command = parse_command(vec![
            "pipe".to_string(),
            "-i".to_string(),
            "file:a.wav".to_string(),
            "-o".to_string(),
            "file:b.mp3".to_string(),
        ])
        .expect("parse command");

        match command {
            Command::Pipe { source, targets } => {
                assert_eq!(source, SourceSpec::File(PathBuf::from("a.wav")));
                assert_eq!(targets, vec![TargetSpec::File(PathBuf::from("b.mp3"))]);
            }
            _ => panic!("expected pipe command"),
        }
    }

    #[test]
    fn parse_pipe_named_devices() {
        let command = parse_command(vec![
            "pipe".to_string(),
            "-i".to_string(),
            "device:usb-mic".to_string(),
            "-o".to_string(),
            "device:airpods".to_string(),
        ])
        .expect("parse command");

        match command {
            Command::Pipe { source, targets } => {
                assert_eq!(source, SourceSpec::Device(Some("usb-mic".to_string())));
                assert_eq!(
                    targets,
                    vec![TargetSpec::Device(Some("airpods".to_string()))]
                );
            }
            _ => panic!("expected pipe command"),
        }
    }

    #[test]
    fn parse_pipe_multiple_outputs() {
        let command = parse_command(vec![
            "pipe".to_string(),
            "-i".to_string(),
            "device:mic".to_string(),
            "-o".to_string(),
            "device:speaker".to_string(),
            "-o".to_string(),
            "file:record.wav".to_string(),
        ])
        .expect("parse command");

        match command {
            Command::Pipe { source, targets } => {
                assert_eq!(source, SourceSpec::Device(Some("mic".to_string())));
                assert_eq!(
                    targets,
                    vec![
                        TargetSpec::Device(Some("speaker".to_string())),
                        TargetSpec::File(PathBuf::from("record.wav")),
                    ]
                );
            }
            _ => panic!("expected pipe command"),
        }
    }

    #[test]
    fn parse_pipe_missing_source_fails() {
        let err = parse_command(vec![
            "pipe".to_string(),
            "-o".to_string(),
            "device".to_string(),
        ])
        .expect_err("missing source should fail");

        assert_eq!(
            err.to_string(),
            "pipe requires -i file:<path> or -i device[:name]"
        );
    }

    #[test]
    fn parse_pipe_missing_target_fails() {
        let err = parse_command(vec![
            "pipe".to_string(),
            "-i".to_string(),
            "file:input.wav".to_string(),
        ])
        .expect_err("missing target should fail");

        assert_eq!(
            err.to_string(),
            "pipe requires at least one -o file:<path> or -o device[:name]"
        );
    }

    #[test]
    fn parse_pipe_unknown_flag_fails() {
        let err = parse_command(vec![
            "pipe".to_string(),
            "-x".to_string(),
            "file:input.wav".to_string(),
            "-o".to_string(),
            "device".to_string(),
        ])
        .expect_err("unknown flag should fail");

        assert_eq!(err.to_string(), "unexpected argument: -x");
    }

    #[test]
    fn parse_pipe_device_with_name_then_flag() {
        let command = parse_command(vec![
            "pipe".to_string(),
            "-i".to_string(),
            "device:my-mic".to_string(),
            "-o".to_string(),
            "device".to_string(),
        ])
        .expect("parse command");

        match command {
            Command::Pipe { source, targets } => {
                assert_eq!(source, SourceSpec::Device(Some("my-mic".to_string())));
                assert_eq!(targets, vec![TargetSpec::Device(None)]);
            }
            _ => panic!("expected pipe command"),
        }
    }

    #[test]
    fn parse_pipe_windows_file_path() {
        let command = parse_command(vec![
            "pipe".to_string(),
            "-i".to_string(),
            "file:E:\\music.wav".to_string(),
            "-o".to_string(),
            "file:E:\\record.wav".to_string(),
        ])
        .expect("parse command");

        match command {
            Command::Pipe { source, targets } => {
                assert_eq!(source, SourceSpec::File(PathBuf::from("E:\\music.wav")));
                assert_eq!(
                    targets,
                    vec![TargetSpec::File(PathBuf::from("E:\\record.wav"))]
                );
            }
            _ => panic!("expected pipe command"),
        }
    }
}
