//! AP-18/19: closed-loop, one-in-flight TCP echo. Not an offered-load fan-out test.
#![forbid(unsafe_code)]
#[allow(dead_code)]
mod perf_support;
use abps_reference::{mosaic as m, serial::SerialBuffer};
use bytes::BytesMut;
use perf_support::{Args, Result, metrics::Latency, payload};
use serde_json::{Value, json};
use std::{io::Write, time::Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    mut io: S,
    mode: &str,
    size: usize,
    frames: usize,
) -> Result<Value> {
    let config = m::Config::new(
        vec![m::ClassConfig {
            capacity: size,
            slots: 1,
        }],
        1,
        usize::MAX,
    );
    let mut storage = if mode == "bytes-retained" {
        None
    } else {
        Some(m::StorageOwner::prepare(&config)?)
    };
    let pool = storage.as_ref().map(|s| s.activate()).transpose()?;
    let opened = pool.as_ref().map(|p| p.try_open_domain(size)).transpose()?;
    let domain = opened.as_ref().map(|(_, d)| d);
    let mut serial = if mode == "serial" {
        Some(SerialBuffer::from_buffer(
            domain.unwrap().try_acquire(size)?,
        ))
    } else {
        None
    };
    let mut bytes = BytesMut::new();
    if mode == "bytes-retained" {
        bytes.resize(size, 0);
    }
    let reserved = if storage.is_some() {
        m::Reservation::for_config(&config)?.total_bytes
    } else {
        0
    };
    let started = Instant::now();
    let result: Result<()> = async {
        for id in 0..frames as u64 {
            if io.read_u64().await? != id {
                return Err("serial frame identity mismatch".into());
            }
            match mode {
                "fresh" => {
                    let mut b = domain.unwrap().try_acquire(size)?;
                    io.read_exact(b.initialize_to(size)?).await?;
                    b.set_filled_len(size)?;
                    io.write_all(b.as_ref()).await?;
                }
                "serial" => {
                    let mut w = serial.as_mut().unwrap().begin();
                    io.read_exact(w.initialize_to(size)?).await?;
                    w.set_filled_len(size)?;
                    let frame = w.finish();
                    io.write_all(frame.as_ref()).await?;
                }
                "bytes-retained" => {
                    io.read_exact(&mut bytes).await?;
                    io.write_all(&bytes).await?;
                }
                _ => return Err("invalid serial TCP mode".into()),
            }
        }
        io.shutdown().await?;
        Ok(())
    }
    .await;
    let elapsed = started.elapsed();
    let idle = domain.map_or(bytes.capacity(), |d| d.metrics().admitted_bytes);
    drop((serial, bytes, opened, pool));
    if let Some(s) = storage.as_mut() {
        s.try_destroy()?;
    }
    // Error/cancellation paths release actual ownership; no fake successful drain.
    result?;
    Ok(
        json!({"mode":mode,"frames":frames,"size":size,"drained":true,
        "seconds":elapsed.as_secs_f64(),"retained_idle_bytes":idle,"arena_bytes":reserved,
        "logical_backing_limit_bytes":size,"model":"closed-loop one in-flight; no queue or fan-out"}),
    )
}
async fn run(args: &Args) -> Result<()> {
    for k in args.0.keys() {
        if !["role", "mode", "address", "size", "iterations"].contains(&k.as_str()) {
            return Err(format!("unknown serial TCP option {k}").into());
        }
    }
    let mode = args.get("mode", "serial");
    if !["fresh", "serial", "bytes-retained"].contains(&mode) {
        return Err("invalid serial TCP mode".into());
    }
    let size = args.n("size", 1500);
    let frames = args.n("iterations", 10000);
    if !(1..=1048576).contains(&size) || !(1..=20000000).contains(&frames) {
        return Err("invalid size/iterations".into());
    }
    if args.get("role", "server") == "server" {
        let listener = tokio::net::TcpListener::bind(args.get("address", "127.0.0.1:0")).await?;
        println!("{}", json!({"ready":listener.local_addr()?.to_string()}));
        std::io::stdout().flush()?;
        let (socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        println!("{}", serve(socket, mode, size, frames).await?);
    } else if args.get("role", "") == "client" {
        let mut socket = tokio::net::TcpStream::connect(args.get("address", "")).await?;
        socket.set_nodelay(true)?;
        let mut source = vec![0; size];
        let mut received = vec![0; size];
        let mut latency = Latency::default();
        let start = Instant::now();
        for id in 0..frames as u64 {
            payload::fill_payload(&mut source, 42, 0, id);
            let at = Instant::now();
            socket.write_u64(id).await?;
            socket.write_all(&source).await?;
            socket.read_exact(&mut received).await?;
            if received != source {
                return Err("serial TCP payload corruption".into());
            }
            latency.record(at.elapsed());
        }
        println!(
            "{}",
            json!({"frames":frames,"size":size,"validated_bytes":frames*size,
            "seconds":start.elapsed().as_secs_f64(),"roundtrip":latency.json(),"schedule":"closed-loop"})
        );
    } else {
        return Err("invalid serial TCP role".into());
    }
    Ok(())
}
fn main() -> Result<()> {
    let args = Args::parse()?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run(&args))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn ap19_serial_tcp_matches_payload_and_retained_charge() {
        for mode in ["fresh", "serial", "bytes-retained"] {
            let (server, mut client) = tokio::io::duplex(16);
            let send = async {
                let mut result = [0; 17];
                for id in 0..3 {
                    client.write_u64(id).await.unwrap();
                    client.write_all(&[id as u8; 17]).await.unwrap();
                    client.read_exact(&mut result).await.unwrap();
                    assert_eq!(result, [id as u8; 17]);
                }
            };
            let (r, ()) = tokio::join!(serve(server, mode, 17, 3), send);
            let r = r.unwrap();
            assert_eq!(
                r["retained_idle_bytes"],
                if mode == "fresh" { 0 } else { 17 }
            );
            assert_eq!(r["drained"], true);
        }
    }
    #[tokio::test]
    async fn ap19_serial_tcp_truncation_is_error_not_completed_frame() {
        for mode in ["fresh", "serial", "bytes-retained"] {
            let (server, mut client) = tokio::io::duplex(32);
            client.write_u64(0).await.unwrap();
            client.write_all(b"partial").await.unwrap();
            drop(client);
            assert!(serve(server, mode, 17, 1).await.is_err());
        }
    }
}
