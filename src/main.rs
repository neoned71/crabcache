mod commands;
mod db;
mod protocol;
mod server;
mod state;

use clap::Parser;
use server::run;
use state::ServerState;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(
    name = "crabcache",
    version,
    about = "Redis-compatible server in Rust (crabcache)"
)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,
    #[arg(long, default_value_t = 6379)]
    port: u16,
    #[arg(long, default_value = ".")]
    dir: String,
    #[arg(long, default_value = "dump.rdb")]
    dbfilename: String,
    #[arg(long, default_value_t = false)]
    appendonly: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let dbm = Arc::new(db::DbManager::new(16));
    let mut state = ServerState::new(dbm, args.bind.clone(), args.port);
    state.dir = args.dir.clone();
    state.dbfilename = args.dbfilename.clone();
    state.appendonly = args.appendonly;

    // AOF setup
    if args.appendonly {
        let aof_path = std::path::Path::new(&args.dir).join(&state.appendfilename);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&aof_path)?;
        let buf = std::io::BufWriter::new(file);
        let aof = Arc::new(parking_lot::Mutex::new(buf));
        state.aof = Some(aof.clone());

        // periodic flush task (everysec, matching Redis default)
        let aof2 = aof.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
            loop {
                interval.tick().await;
                let mut f = aof2.lock();
                use std::io::Write;
                let _ = f.flush();
            }
        });

        // replay
        let mut f = std::fs::File::open(&aof_path)?;
        let mut data = Vec::new();
        f.read_to_end(&mut data)?;
        // parse frames
        let mut ctx = commands::ConnContext::new(0);
        let mut pos = 0;
        while pos < data.len() {
            match protocol::parse(&data[pos..]) {
                Ok(Some((frame, consumed))) => {
                    pos += consumed;
                    let _ = commands::dispatch(&state, &mut ctx, frame).await;
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
    }

    let state = Arc::new(state);
    run(state).await
}
