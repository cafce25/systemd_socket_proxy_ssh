use std::{
    ffi::c_int,
    os::fd::{BorrowedFd, FromRawFd},
    pin::Pin,
    time::Duration,
};

use anyhow::{Context, Result};
use clap::Parser;
use futures::{FutureExt, StreamExt, future, stream::FuturesUnordered};
use nix::{
    fcntl::{self, FcntlArg, OFlag},
    libc::O_NONBLOCK,
};
use systemd::daemon::{Listening, SocketType};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::UnixSocket,
    process::Command,
};

struct HookRm(String);
impl Drop for HookRm {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Parameters::try_parse().context("parsing parameters")?;
    let listeners = systemd::daemon::listen_fds(true)?
        .iter()
        // Safety:
        // - `fd` is a file descriptor received through `sd_listen_fds`
        .map(|fd| unsafe { get_socket(fd) })
        .collect::<Result<Vec<_>>>()
        .context("getting sockets")?;

    if listeners.len() != args.ports.len() {
        eprintln!(
            "WARN: mismatch of sockets passed in by systemd {} and ports specified on commandline {}",
            listeners.len(),
            args.ports.len()
        );
    }

    let unit = std::env::var("SYSTEMD_UNIT").unwrap_or("unknown_unit".to_string());

    let folder = format!("/run/user/1000/systemd/units/{unit}");
    let _f = HookRm(folder.clone());
    tokio::fs::create_dir_all(&folder)
        .await
        .context("create dir")?;

    let sockets = (0..args.ports.len())
        .map(|i| format!("{folder}/{i}.sock"))
        .collect::<Vec<_>>();

    let mut ssh = Command::new("ssh")
        .args(["-N"])
        .arg(&args.host)
        .args(
            std::iter::zip(&sockets, &args.ports)
                .map(|(socket, port)| format!("-L{socket}:{port}")),
        )
        .spawn()
        .context("spawning ssh")?;

    let ssh = tokio::spawn(async move { ssh.wait().await });
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut futures = std::iter::zip(listeners, sockets)
        .map(|(listener, socket)| handle_listener_socket(listener, socket))
        .map(tokio::spawn)
        .collect::<FuturesUnordered<_>>();

    while let Some(f) = futures.next().await {
        f??;
    }

    // future::try_join_all(futures)
    //     .await
    //     .context("joining connection handling futures")?;

    ssh.await.context("running ssh")?.unwrap();

    Ok(())
}

async fn handle_listener_socket(mut listener: Listener, socket: String) -> Result<()> {
    eprintln!(
        "notice: started listening for connections on {listener:?} and forwarding them to {socket}"
    );
    let mut copiers = std::pin::pin!(FuturesUnordered::new());
    let mut next_copier = copiers.next().fuse();
    loop {
        futures::select! {
            _a = next_copier => {println!("copying done");},
            s = listener.accept().fuse() => {
                match s {
                    Ok(mut connection) => {
                        println!("new copying task");
                        let mut ssh_conn = UnixSocket::new_stream()?.connect(&socket).await.context("connect to unix socket")?;
                        println!("ssh_conn = {ssh_conn:?}");
                        copiers.push( tokio::spawn(async move { tokio::io::copy_bidirectional(&mut connection, &mut ssh_conn).await } ));
                        next_copier = copiers.next().fuse();

                    }
                    Err(_e) => {
                        future::join_all(copiers.as_mut().iter_pin_mut()).await;
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// # Safety:
/// `fd` must be a file descriptor that was received from `sd_listen_fds` or
/// `sd_listen_fds_with_names`
unsafe fn get_socket(fd: c_int) -> Result<Listener> {
    let socket = if systemd::daemon::is_socket(
        fd,
        None,
        Some(SocketType::Stream),
        Listening::IsListening,
    )? {
        {
            let fd = unsafe { BorrowedFd::borrow_raw(fd) };
            let flags = fcntl::fcntl(fd, FcntlArg::F_GETFL)?;
            fcntl::fcntl(
                fd,
                FcntlArg::F_SETFL(OFlag::from_bits(flags | O_NONBLOCK).unwrap()),
            )?;
        }
        if systemd::daemon::is_socket_inet(
            fd,
            None,
            Some(SocketType::Stream),
            Listening::IsListening,
            None,
        )? {
            // Safety:
            // - The socket was passed from systemd
            // - It's confirmed to be a TCP (streaming inet) socket
            // - It's also confirmed to be a TcpListener (it's in listening mode)
            let std_socket = unsafe { std::net::TcpListener::from_raw_fd(fd) };
            Listener::Tcp(tokio::net::TcpListener::from_std(std_socket)?)
        } else if systemd::daemon::is_socket_unix(fd, None, Listening::IsListening, None::<&str>)? {
            // Safety:
            // - The socket was passed from systemd
            // - It's confirmed to be a Unix (streaming unix) socket
            // - It's also confirmed to be a UnixListener (it's in listening mode)
            let std_socket = unsafe { std::os::unix::net::UnixListener::from_raw_fd(fd) };
            Listener::Unix(tokio::net::UnixListener::from_std(std_socket)?)
        } else {
            todo!("sockets other than inet or unix are not yet supported")
        }
    } else {
        todo!("file descriptors other than sockets are not yet supported")
    };
    Ok(dbg!(socket))
}

#[derive(clap::Parser)]
struct Parameters {
    host: String,
    ports: Vec<String>,
}

#[derive(Debug)]
enum Listener {
    Tcp(tokio::net::TcpListener),
    Unix(tokio::net::UnixListener),
}

impl Listener {
    async fn accept(&mut self) -> Result<Stream> {
        Ok(match self {
            Listener::Tcp(tcp) => {
                let (socket, addr) = tcp.accept().await?;
                eprintln!("new connection to {tcp:?} from {addr:?}");
                Stream::Tcp(socket)
            }
            Listener::Unix(unix) => {
                let (socket, addr) = unix.accept().await?;
                eprintln!("new connection to {unix:?} from {addr:?}");
                Stream::Unix(socket)
            }
        })
    }
}

#[derive(Debug)]
enum Stream {
    Tcp(tokio::net::TcpStream),
    Unix(tokio::net::UnixStream),
}

#[derive(Debug)]
enum StreamProject<'a> {
    Tcp(Pin<&'a mut tokio::net::TcpStream>),
    Unix(Pin<&'a mut tokio::net::UnixStream>),
}

impl Stream {
    fn project(self: Pin<&mut Self>) -> StreamProject<'_> {
        match unsafe { self.get_unchecked_mut() } {
            Stream::Tcp(tcp) => StreamProject::Tcp(unsafe { Pin::new_unchecked(tcp) }),
            Stream::Unix(unix) => StreamProject::Unix(unsafe { Pin::new_unchecked(unix) }),
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.project() {
            StreamProject::Tcp(tcp) => tcp.poll_read(cx, buf),
            StreamProject::Unix(unix) => unix.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
        match self.project() {
            StreamProject::Tcp(tcp) => tcp.poll_write(cx, buf),
            StreamProject::Unix(unix) => unix.poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match self.project() {
            StreamProject::Tcp(tcp) => tcp.poll_flush(cx),
            StreamProject::Unix(unix) => unix.poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match self.project() {
            StreamProject::Tcp(tcp) => tcp.poll_shutdown(cx),
            StreamProject::Unix(unix) => unix.poll_shutdown(cx),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
        match self.project() {
            StreamProject::Tcp(tcp) => tcp.poll_write_vectored(cx, bufs),
            StreamProject::Unix(unix) => unix.poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Stream::Tcp(tcp) => tcp.is_write_vectored(),
            Stream::Unix(unix) => unix.is_write_vectored(),
        }
    }
}
