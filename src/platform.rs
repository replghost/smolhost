// SPDX-License-Identifier: GPL-3.0-or-later

//! Add certificate-verified WSS peer connections to smoldot's native platform.
//! All execution, clocks, randomness, logging, and non-TLS transports stay upstream.

use std::{
    borrow::Cow,
    fmt, io,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use futures_rustls::{
    TlsConnector,
    client::TlsStream,
    rustls::{self, ClientConfig, RootCertStore, pki_types::ServerName},
};
use futures_util::{FutureExt, future};
use smoldot::libp2p::websocket;
use smoldot_light::platform::{
    Address, ConnectionType, LogLevel, MultiStreamAddress, PlatformRef, SubstreamDirection,
    default::{self, DefaultPlatform},
    with_buffers,
};

type Native = Arc<DefaultPlatform>;
type SecureSocket = websocket::Connection<TlsStream<async_net::TcpStream>>;
type SecureBuffers = with_buffers::WithBuffers<
    future::BoxFuture<'static, Result<SecureSocket, io::Error>>,
    SecureSocket,
    Instant,
>;

pub struct Platform {
    native: Native,
    // No user callbacks or mutable configuration; each handshake owns its state.
    // Rustls trait objects do not declare the unwind-safety bound smoldot requires.
    tls: AssertUnwindSafe<Arc<ClientConfig>>,
}

impl Clone for Platform {
    fn clone(&self) -> Self {
        Self {
            native: self.native.clone(),
            tls: AssertUnwindSafe(self.tls.0.clone()),
        }
    }
}

impl Platform {
    pub fn new() -> Self {
        let roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .expect("Rustls default TLS versions")
                .with_root_certificates(roots)
                .with_no_client_auth();
        Self {
            native: DefaultPlatform::new(
                env!("CARGO_PKG_NAME").into(),
                env!("CARGO_PKG_VERSION").into(),
            ),
            tls: AssertUnwindSafe(Arc::new(tls)),
        }
    }
}

#[pin_project::pin_project(project = StreamProjection)]
pub enum Stream {
    Native {
        #[pin]
        stream: default::Stream,
    },
    Secure {
        #[pin]
        stream: SecureBuffers,
    },
}

fn native_stream(stream: default::Stream) -> Stream {
    Stream::Native { stream }
}

impl PlatformRef for Platform {
    type Delay = <Native as PlatformRef>::Delay;
    type Instant = Instant;
    type MultiStream = <Native as PlatformRef>::MultiStream;
    type Stream = Stream;
    type StreamConnectFuture = future::Either<
        future::Ready<Stream>,
        future::Map<<Native as PlatformRef>::StreamConnectFuture, fn(default::Stream) -> Stream>,
    >;
    type MultiStreamConnectFuture = <Native as PlatformRef>::MultiStreamConnectFuture;
    type ReadWriteAccess<'a> = with_buffers::ReadWriteAccess<'a, Instant>;
    type StreamUpdateFuture<'a> = future::BoxFuture<'a, ()>;
    type StreamErrorRef<'a> = &'a io::Error;
    type NextSubstreamFuture<'a> = future::Pending<Option<(Self::Stream, SubstreamDirection)>>;

    fn now_from_unix_epoch(&self) -> Duration {
        self.native.now_from_unix_epoch()
    }
    fn now(&self) -> Instant {
        self.native.now()
    }
    fn fill_random_bytes(&self, buffer: &mut [u8]) {
        self.native.fill_random_bytes(buffer);
    }
    fn sleep(&self, duration: Duration) -> Self::Delay {
        self.native.sleep(duration)
    }
    fn sleep_until(&self, when: Instant) -> Self::Delay {
        self.native.sleep_until(when)
    }
    fn spawn_task(&self, name: Cow<str>, task: impl Future<Output = ()> + Send + 'static) {
        self.native.spawn_task(name, task);
    }
    fn log<'a>(
        &self,
        level: LogLevel,
        target: &'a str,
        message: &'a str,
        values: impl Iterator<Item = (&'a str, &'a dyn fmt::Display)>,
    ) {
        self.native.log(level, target, message, values);
    }
    fn client_name(&self) -> Cow<'_, str> {
        self.native.client_name()
    }
    fn client_version(&self) -> Cow<'_, str> {
        self.native.client_version()
    }
    fn supports_connection_type(&self, ty: ConnectionType) -> bool {
        matches!(ty, ConnectionType::WebSocketDns { secure: true, .. })
            || self.native.supports_connection_type(ty)
    }

    fn connect_stream(&self, address: Address) -> Self::StreamConnectFuture {
        if let Address::WebSocketDns {
            hostname,
            port,
            secure: true,
        } = address
        {
            let hostname = hostname.to_owned();
            let tls = self.tls.0.clone();
            let connect = async move {
                let tcp = async_net::TcpStream::connect((hostname.as_str(), port)).await?;
                tcp.set_nodelay(true)?;
                let server_name = ServerName::try_from(hostname.clone())
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
                let stream = TlsConnector::from(tls).connect(server_name, tcp).await?;
                websocket::websocket_client_handshake(websocket::Config {
                    tcp_socket: stream,
                    host: &format!("{hostname}:{port}"),
                    url: "/",
                })
                .await
            }
            .boxed();
            future::Either::Left(future::ready(Stream::Secure {
                stream: with_buffers::WithBuffers::new(connect),
            }))
        } else {
            future::Either::Right(
                self.native
                    .connect_stream(address)
                    .map(native_stream as fn(_) -> _),
            )
        }
    }

    fn connect_multistream(&self, address: MultiStreamAddress) -> Self::MultiStreamConnectFuture {
        self.native.connect_multistream(address)
    }
    fn open_out_substream(&self, connection: &mut Self::MultiStream) {
        match *connection {}
    }
    fn next_substream<'a>(
        &self,
        connection: &'a mut Self::MultiStream,
    ) -> Self::NextSubstreamFuture<'a> {
        match *connection {}
    }
    fn read_write_access<'a>(
        &self,
        stream: Pin<&'a mut Stream>,
    ) -> Result<Self::ReadWriteAccess<'a>, &'a io::Error> {
        match stream.project() {
            StreamProjection::Native { stream } => self.native.read_write_access(stream),
            StreamProjection::Secure { stream } => stream.read_write_access(self.now()),
        }
    }
    fn wait_read_write_again<'a>(
        &self,
        stream: Pin<&'a mut Stream>,
    ) -> Self::StreamUpdateFuture<'a> {
        match stream.project() {
            StreamProjection::Native { stream } => self.native.wait_read_write_again(stream),
            StreamProjection::Secure { stream } => {
                let native = self.native.clone();
                Box::pin(stream.wait_read_write_again(move |when| native.sleep_until(when)))
            }
        }
    }
}
