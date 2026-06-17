use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use airs_audio::{AudioInput, AudioOutput, AudioSink, list_audio_devices};
use futures::SinkExt;
use tokio_stream::StreamExt;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Source {
    Device(Option<String>),
    File(PathBuf),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Target {
    Device(Option<String>),
    File(PathBuf),
}

#[derive(Debug)]
enum Command {
    Help,
    Version,
    Devices,
    Pipe {
        source: Source,
        targets: Vec<Target>,
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
        Command::Version => cmd_version(),
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
            "-i:d" => {
                if source.is_some() {
                    return Err(invalid_input("-i can only be used once"));
                }
                let name = peek_value(args, &mut i);
                source = Some(Source::Device(name));
            }
            "-i:f" => {
                i += 1;
                let path = args
                    .get(i)
                    .ok_or_else(|| invalid_input("-i:f requires a file path"))?;
                if source.is_some() {
                    return Err(invalid_input("-i can only be used once"));
                }
                source = Some(Source::File(PathBuf::from(path)));
            }
            "-o:d" => {
                let name = peek_value(args, &mut i);
                targets.push(Target::Device(name));
            }
            "-o:f" => {
                i += 1;
                let path = args
                    .get(i)
                    .ok_or_else(|| invalid_input("-o:f requires a file path"))?;
                targets.push(Target::File(PathBuf::from(path)));
            }
            arg => return Err(invalid_input(format!("unexpected argument: {arg}"))),
        }
        i += 1;
    }

    let source = source.ok_or_else(|| invalid_input("pipe requires -i:d or -i:f"))?;
    if targets.is_empty() {
        return Err(invalid_input("pipe requires at least one -o:d or -o:f"));
    }

    Ok(Command::Pipe { source, targets })
}

/// Peek at the next argument; if it looks like a value (not a flag), consume it.
fn peek_value(args: &[String], i: &mut usize) -> Option<String> {
    if let Some(next) = args.get(*i + 1) {
        if !next.starts_with('-') {
            *i += 1;
            return Some(next.clone());
        }
    }
    None
}

fn cmd_help() {
    println!("Usage:");
    println!("  airs-audio --help");
    println!("  airs-audio --version");
    println!("  airs-audio list_devices");
    println!("  airs-audio pipe -i:d [device] -i:f <file> -o:d [device] -o:f <file>");
    println!();
    println!("  -i:d          Default input device");
    println!("  -i:d <name>   Named input device");
    println!("  -i:f <path>   Input file");
    println!("  -o:d          Default output device");
    println!("  -o:d <name>   Named output device");
    println!("  -o:f <path>   Output file");
    println!();
    println!("Examples:");
    println!("  airs-audio pipe -i:f music.wav -o:d");
    println!("  airs-audio pipe -i:d -o:f record.wav");
    println!("  airs-audio pipe -i:d mic -o:d speaker -o:f record.wav");
}

fn cmd_version() {
    println!("{}", airs_audio::version());
}

fn cmd_list_devices() -> Result<(), Box<dyn Error>> {
    let devices = list_audio_devices()?;

    println!("Input devices:");
    for device in devices.inputs {
        if device.is_default {
            println!("{} (default)", device.name);
        } else {
            println!("{}", device.name);
        }
    }

    println!();
    println!("Output devices:");
    for device in devices.outputs {
        if device.is_default {
            println!("{} (default)", device.name);
        } else {
            println!("{}", device.name);
        }
    }

    Ok(())
}

async fn cmd_pipe(source: Source, targets: Vec<Target>) -> Result<(), Box<dyn Error>> {
    let is_device_source = matches!(source, Source::Device(_));
    let mut input_stream = match source {
        Source::Device(None) => AudioInput::default_device().open()?,
        Source::Device(Some(name)) => AudioInput::device(name).open()?,
        Source::File(path) => AudioInput::file(path).open()?,
    };

    let stop = Arc::new(AtomicBool::new(false));
    if is_device_source {
        let stop_signal = stop.clone();
        ctrlc::set_handler(move || {
            stop_signal.store(true, Ordering::SeqCst);
        })?;
        eprintln!("Piping audio. Press Ctrl+C to stop.");
    }

    let mut sinks: Vec<AudioSink> = targets
        .iter()
        .map(|target| match target {
            Target::Device(None) => AudioOutput::default_device().open(),
            Target::Device(Some(name)) => AudioOutput::device(name.clone()).open(),
            Target::File(path) => AudioOutput::file(path).open(),
        })
        .collect::<Result<Vec<_>, _>>()?;

    while !stop.load(Ordering::SeqCst) {
        match input_stream.next().await {
            Some(frame) => {
                let frame = frame?;
                for sink in &mut sinks {
                    sink.send(frame.clone()).await?;
                }
            }
            None => break,
        }
    }

    for sink in &mut sinks {
        sink.close().await?;
    }
    print_written_files(targets);
    Ok(())
}

fn print_written_files(targets: Vec<Target>) {
    for target in targets {
        if let Target::File(path) = target {
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
            "-i:d".to_string(),
            "-o:d".to_string(),
        ])
        .expect("parse command");

        match command {
            Command::Pipe { source, targets } => {
                assert_eq!(source, Source::Device(None));
                assert_eq!(targets, vec![Target::Device(None)]);
            }
            _ => panic!("expected pipe command"),
        }
    }

    #[test]
    fn parse_pipe_device_to_file() {
        let command = parse_command(vec![
            "pipe".to_string(),
            "-i:d".to_string(),
            "-o:f".to_string(),
            "record.wav".to_string(),
        ])
        .expect("parse command");

        match command {
            Command::Pipe { source, targets } => {
                assert_eq!(source, Source::Device(None));
                assert_eq!(targets, vec![Target::File(PathBuf::from("record.wav"))]);
            }
            _ => panic!("expected pipe command"),
        }
    }

    #[test]
    fn parse_pipe_file_to_device() {
        let command = parse_command(vec![
            "pipe".to_string(),
            "-i:f".to_string(),
            "music.wav".to_string(),
            "-o:d".to_string(),
        ])
        .expect("parse command");

        match command {
            Command::Pipe { source, targets } => {
                assert_eq!(source, Source::File(PathBuf::from("music.wav")));
                assert_eq!(targets, vec![Target::Device(None)]);
            }
            _ => panic!("expected pipe command"),
        }
    }

    #[test]
    fn parse_pipe_file_to_file() {
        let command = parse_command(vec![
            "pipe".to_string(),
            "-i:f".to_string(),
            "a.wav".to_string(),
            "-o:f".to_string(),
            "b.mp3".to_string(),
        ])
        .expect("parse command");

        match command {
            Command::Pipe { source, targets } => {
                assert_eq!(source, Source::File(PathBuf::from("a.wav")));
                assert_eq!(targets, vec![Target::File(PathBuf::from("b.mp3"))]);
            }
            _ => panic!("expected pipe command"),
        }
    }

    #[test]
    fn parse_pipe_named_devices() {
        let command = parse_command(vec![
            "pipe".to_string(),
            "-i:d".to_string(),
            "usb-mic".to_string(),
            "-o:d".to_string(),
            "airpods".to_string(),
        ])
        .expect("parse command");

        match command {
            Command::Pipe { source, targets } => {
                assert_eq!(source, Source::Device(Some("usb-mic".to_string())));
                assert_eq!(targets, vec![Target::Device(Some("airpods".to_string()))]);
            }
            _ => panic!("expected pipe command"),
        }
    }

    #[test]
    fn parse_pipe_multiple_outputs() {
        let command = parse_command(vec![
            "pipe".to_string(),
            "-i:d".to_string(),
            "mic".to_string(),
            "-o:d".to_string(),
            "speaker".to_string(),
            "-o:f".to_string(),
            "record.wav".to_string(),
        ])
        .expect("parse command");

        match command {
            Command::Pipe { source, targets } => {
                assert_eq!(source, Source::Device(Some("mic".to_string())));
                assert_eq!(
                    targets,
                    vec![
                        Target::Device(Some("speaker".to_string())),
                        Target::File(PathBuf::from("record.wav")),
                    ]
                );
            }
            _ => panic!("expected pipe command"),
        }
    }

    #[test]
    fn parse_pipe_missing_source_fails() {
        let err = parse_command(vec!["pipe".to_string(), "-o:d".to_string()])
            .expect_err("missing source should fail");

        assert_eq!(err.to_string(), "pipe requires -i:d or -i:f");
    }

    #[test]
    fn parse_pipe_missing_target_fails() {
        let err = parse_command(vec![
            "pipe".to_string(),
            "-i:f".to_string(),
            "input.wav".to_string(),
        ])
        .expect_err("missing target should fail");

        assert_eq!(err.to_string(), "pipe requires at least one -o:d or -o:f");
    }

    #[test]
    fn parse_pipe_unknown_flag_fails() {
        let err = parse_command(vec![
            "pipe".to_string(),
            "-i:x".to_string(),
            "input.wav".to_string(),
            "-o:d".to_string(),
        ])
        .expect_err("unknown flag should fail");

        assert_eq!(err.to_string(), "unexpected argument: -i:x");
    }

    #[test]
    fn parse_pipe_device_with_name_then_flag() {
        let command = parse_command(vec![
            "pipe".to_string(),
            "-i:d".to_string(),
            "my-mic".to_string(),
            "-o:d".to_string(),
        ])
        .expect("parse command");

        match command {
            Command::Pipe { source, targets } => {
                assert_eq!(source, Source::Device(Some("my-mic".to_string())));
                assert_eq!(targets, vec![Target::Device(None)]);
            }
            _ => panic!("expected pipe command"),
        }
    }
}
