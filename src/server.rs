use crate::commands::{ConnContext, dispatch};
use crate::protocol;
use crate::state::ServerState;
use bytes::{Buf, BytesMut};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

pub async fn run(state: Arc<ServerState>) -> anyhow::Result<()> {
    let addr = format!("{}:{}", state.bind, state.port);
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("redisrs listening on {}", addr);
    loop {
        let (socket, peer) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(state, socket).await {
                tracing::debug!("conn {} closed: {}", peer, e);
            }
        });
    }
}

async fn handle_conn(state: Arc<ServerState>, socket: TcpStream) -> anyhow::Result<()> {
    let (mut rd, mut wr) = socket.into_split();
    let mut ctx = ConnContext::new(next_id(&state));
    let mut buf = BytesMut::with_capacity(4096);

    // writer channel
    let (wtx, mut wrx) = mpsc::unbounded_channel::<BytesMut>();
    let writer = tokio::spawn(async move {
        while let Some(mut data) = wrx.recv().await {
            if wr.write_all(&mut data).await.is_err() {
                break;
            }
        }
    });

    // broadcast receiver for pub/sub
    let mut bcast_rx = state.pubsub.global.subscribe();

    loop {
        tokio::select! {
            biased;
            // broadcast messages
            msg = bcast_rx.recv() => {
                if let Ok((ch, message)) = msg {
                    if ctx.in_subscribed_mode && ctx.subscribed_channels.iter().any(|c| c == &ch) {
                        let mut out = BytesMut::new();
                        let frame = protocol::Frame::Array(Some(vec![
                            protocol::Frame::BulkString(Some(bytes::Bytes::from("message"))),
                            protocol::Frame::BulkString(Some(ch)),
                            protocol::Frame::BulkString(Some(message)),
                        ]));
                        frame.serialize(&mut out);
                        let _ = wtx.send(out);
                    }
                }
            }
            // socket read
            n = rd.read_buf(&mut buf) => {
                if n? == 0 {
                    break;
                }
                while let Some((frame, consumed)) = protocol::parse(&buf).map_err(|e| anyhow::anyhow!(e))? {
                    buf.advance(consumed);
                    // AOF log
                    if state.appendonly {
                        if let Some(aof) = &state.aof {
                            let mut buf2 = BytesMut::new();
                            frame.serialize(&mut buf2);
                            let mut f = aof.lock();
                            use std::io::Write;
                            let _ = f.write_all(&buf2);
                            let _ = f.flush();
                        }
                    }
                    let replies = dispatch(&state, &mut ctx, frame).await;
                    let mut out = BytesMut::new();
                    for r in &replies {
                        r.serialize(&mut out);
                    }
                    let _ = wtx.send(out);

                    // if newly subscribed, create/ensure broadcast receiver already global (no-op)
                    if ctx.in_subscribed_mode && !ctx.subscribed_channels.is_empty() {
                        // ensure bcast_rx active (already)
                    }
                }
            }
        }
    }

    drop(wtx);
    let _ = writer.await;
    Ok(())
}

fn next_id(state: &ServerState) -> u64 {
    let mut g = state.next_client_id.write();
    let id = *g;
    *g += 1;
    id
}
