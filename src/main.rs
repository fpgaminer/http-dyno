extern crate tokio;
#[macro_use]
extern crate clap;
extern crate http;
extern crate httparse;
#[macro_use]
extern crate futures;
extern crate bytes;


use std::net::ToSocketAddrs;
use tokio::prelude::*;
use bytes::BytesMut;
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex};


fn main() {
	let matches = clap::App::new("http-dyno")
		.version(crate_version!())
		.setting(clap::AppSettings::ColoredHelp)
		.setting(clap::AppSettings::UnifiedHelpMessage)
		.args_from_usage(
			"-c, --connections=<CONNECTIONS> 'Number of concurrent connections'
			-d, --duration=<DURATION>        'Duration of test, in seconds'
			<SERVER>                         'server to benchmark'")
		.get_matches();
	
	let server = matches.value_of("SERVER").unwrap();
	let num_connections: u64 = matches.value_of("connections").unwrap().parse().unwrap();
	let duration: u64 = matches.value_of("duration").unwrap().parse().unwrap();

	let mut request = http::Request::get("http://".to_string() + &server + "/plaintext");
	let request = request.body(()).unwrap();
	let request_bytes = request_to_bytes(&request);

	let server_addr = request.uri().host().unwrap().to_string() + ":" + &request.uri().port().unwrap_or(80).to_string();
	let addr = server_addr.to_socket_addrs().unwrap().next().unwrap();

	let stats = Arc::new(Mutex::new(0));
	let future_stats = stats.clone();

	tokio::run(future::ok(())
		.and_then(move |_| {
			let deadline = Instant::now() + Duration::from_secs(duration);

			for _ in 0..num_connections {
				let inner_request_bytes = request_bytes.clone();
				let inner_stats = future_stats.clone();
				let client = tokio::net::TcpStream::connect(&addr)
					.map_err(|err| {
						eprintln!("connection error: {}", err)
					})
					.and_then(move |socket| {
						Hammer::new(socket, inner_request_bytes, deadline)
						.map_err(|err| {
							eprintln!("error: {}", err)
						})
					})
					.map(move |responses| {
						*inner_stats.lock().unwrap() += responses;
						()
					});
				
				tokio::spawn(client);
			}

			future::ok(())
		}));
	
	let total_responses = *stats.lock().unwrap();

	println!("Total responses: {}", total_responses);
	println!("Req/s: {}s", total_responses as f64 / duration as f64);
}


fn request_to_bytes(request: &http::Request<()>) -> Vec<u8> {
	let mut result = Vec::new();

	// Request line
	result.extend_from_slice(request.method().as_str().as_bytes());
	result.extend_from_slice(b" ");
	result.extend_from_slice(request.uri().path().as_bytes());
	// We always use HTTP/1.1; ignoring request.version
	result.extend_from_slice(b" HTTP/1.1\r\n");

	// Always include Host header
	result.extend_from_slice(("Host: ".to_string() + request.uri().host().unwrap()).as_bytes());
	result.extend_from_slice(b"\r\n");

	// request headers
	for (header_name, header_value) in request.headers() {
		result.extend_from_slice(header_name.as_str().as_bytes());
		result.extend_from_slice(b": ");
		result.extend_from_slice(header_value.as_bytes());
		result.extend_from_slice(b"\r\n");
	}

	// No body
	result.extend_from_slice(b"\r\n");

	result
}


struct Hammer {
	reader: ResponseReader<tokio::io::ReadHalf<tokio::net::TcpStream>>,
	writer: tokio::io::WriteHalf<tokio::net::TcpStream>,
	deadline: tokio::timer::Delay,
	request: Vec<u8>,
	write_pos: usize,
	response_count: u64,
}

impl Hammer {
	fn new(socket: tokio::net::TcpStream, request: Vec<u8>, deadline: Instant) -> Hammer {
		let (reader, writer) = socket.split();

		Hammer {
			reader: ResponseReader {
				socket: reader,
				buffer: BytesMut::new(),
				scan_pos: 0,
				in_progress: None,
			},
			writer: writer,
			deadline: tokio::timer::Delay::new(deadline),
			request: request,
			write_pos: 0,
			response_count: 0,
		}
	}

	fn poll_reader(&mut self) -> Poll<Option<http::Response<BytesMut>>, tokio::io::Error> {
		loop {
			let response = try_ready!(self.reader.poll());

			return Ok(Async::Ready(response));
		}
	}

	fn poll_writer(&mut self) -> Poll<(), tokio::io::Error> {
		while self.write_pos != self.request.len() {
			let n = try_ready!(self.writer.poll_write(&self.request[self.write_pos..]));

			assert!(n > 0);

			self.write_pos += n;
		}

		Ok(Async::Ready(()))
	}
}

impl Future for Hammer {
	type Item = u64;
	type Error = tokio::io::Error;

	fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
		// Check timer
		if self.deadline.poll().map_err(|err| tokio::io::Error::new(tokio::io::ErrorKind::Other, err))?.is_ready() {
			return Ok(Async::Ready(self.response_count));
		}

		// Initially poll the request writer
		let mut expecting_response = self.poll_writer()?.is_ready();

		// Try to read any responses
		loop {
			match self.poll_reader()? {
				Async::Ready(None) => return Err(tokio::io::Error::new(tokio::io::ErrorKind::Other, "socket unexpectedly closed")),
				Async::NotReady => return Ok(Async::NotReady),        // No response yet
				Async::Ready(Some(_response)) => {
					if !expecting_response {
						// There are no pending requests to which we expect a response right now
						return Err(tokio::io::Error::new(tokio::io::ErrorKind::Other, "Server sent an unexpected response"));
					}

					self.response_count += 1;

					// Set up the next request
					self.write_pos = 0;
					// Poll writer again
					expecting_response = self.poll_writer()?.is_ready();
				},
			}
		}
	}
}


struct ResponseReader<S> {
	socket: S,
	buffer: BytesMut,
	scan_pos: usize,
	in_progress: Option<(usize, u16, Vec<(http::header::HeaderName, http::header::HeaderValue)>)>,
}

impl<S: AsyncRead> ResponseReader<S> {
	fn fill_read_buf(&mut self) -> Poll<(), tokio::io::Error> {
		loop {
			self.buffer.reserve(1024);
			let n = try_ready!(self.socket.read_buf(&mut self.buffer));

			if n == 0 {
				return Ok(Async::Ready(()));
			}
		}
	}
}

impl<S: AsyncRead> Stream for ResponseReader<S> {
	type Item = http::Response<BytesMut>;
	type Error = tokio::io::Error;

	fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
		let sock_closed = self.fill_read_buf()?.is_ready();
		if sock_closed {
			return Ok(Async::Ready(None));
		}

		loop {
			if let Some(progress) = self.in_progress.take() {
				if self.buffer.len() >= progress.0 {
					let body = self.buffer.split_to(progress.0);
					let mut response = http::Response::builder();
					response.status(progress.1);

					for (name, value) in &progress.2 {
						response.header(name.to_owned(), value.to_owned());
					}

					let response = response.body(body).unwrap();

					return Ok(Async::Ready(Some(response)));
				}
				else {
					self.in_progress = Some(progress);
					break;
				}
			}
			else if let Some(newline) = self.buffer[self.scan_pos..].iter().position(|&x| x == b'\n') {
				let newline = newline + self.scan_pos;
				let is_empty_line = (newline >= 1 && self.buffer[newline-1] == b'\n') || (newline >= 2 && self.buffer[newline-1] == b'\r' && self.buffer[newline-2] == b'\n');

				if is_empty_line {
					let header = self.buffer.split_to(newline + 1);

					// Parse header
					let (content_length, status, headers) = {
						let mut headers = [httparse::EMPTY_HEADER; 16];
						let mut response = httparse::Response::new(&mut headers);
						response.parse(&header).unwrap().unwrap();
						let http_headers: Vec<_> = response.headers.iter().map(|header| {
							(http::header::HeaderName::from_bytes(header.name.as_bytes()).unwrap(), http::header::HeaderValue::from_bytes(header.value).unwrap())
						}).collect();

						let content_length = http_headers.iter().find(|header| header.0 == "content-length").unwrap().1.to_str().unwrap().parse().unwrap();

						(content_length, response.code.unwrap(), http_headers)
					};

					self.scan_pos = 0;
					self.in_progress = Some((content_length, status, headers));
				}
				else {
					self.scan_pos = newline + 1;
				}
			}
			else {
				self.scan_pos = self.buffer.len();
				break;
			}
		}

		Ok(Async::NotReady)
	}
}