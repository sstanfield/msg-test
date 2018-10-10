extern crate tokio;
extern crate tokio_io;
extern crate bytes;
extern crate futures;

#[macro_use]
extern crate serde_derive;

extern crate serde;
extern crate serde_json;

use tokio::prelude::*;
use tokio::net::TcpListener;
use bytes::{BufMut, BytesMut};
use tokio_io::_tokio_codec::{Encoder, Decoder};
use std::{io, str};
use futures::sync::mpsc;
use std::collections::HashMap;
use std::error::Error;

#[derive(Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Message {
    topic: String,
    payload_size: usize,
    checksum: String,
    #[serde(skip)]
    sequence: usize,
    #[serde(skip)]
    payload: Vec<u8>
}

#[derive(Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ClientTopics {
    topics: Vec<String>
}

#[derive(Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Status {
    status: String
}

pub enum ClientIncoming {
    Topic(ClientTopics),
    Status(Status),
}

pub enum ClientMessage {
    StatusOk,
    StatusError(u32, String),
    Message(Message),
    Over
}

pub enum BrokerMessage {
    Message(Message),
    NewClient(ClientTopics, mpsc::Sender<ClientMessage>),
}

/// A simple `Codec` implementation that reads incoming messages.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct InMessageCodec {
    message: Option<Message>,
}

impl InMessageCodec {
    pub fn new() -> InMessageCodec {
        InMessageCodec { message: None }
    }
}

fn last_brace(buf: &[u8]) -> Option<usize> {
    let mut opens = 0;
    let mut count = 0;
    for b in buf {
        if *b == b'{' { opens += 1; }
        if *b == b'}' { opens -= 1; }
        if opens == 0 { return Some(count) };
        count += 1;
    }
    None
}

impl Decoder for InMessageCodec {
    type Item = Message;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Message>, io::Error> {
        let result: Result<Option<Message>, io::Error>;
        if self.message.is_none() {
            if let Some(brace_offset) = buf[..].iter().position(|b| *b == b'{') {
                if brace_offset > 0 {
                    buf.advance(brace_offset);
                }
            }
            //if let Some(message_offset) = buf[..].iter().position(|b| *b == b'\n') {
            if let Some(message_offset) = last_brace(&buf[..]) {
                if message_offset > 3 {
                    let line = buf.split_to(message_offset + 1);
                    //println!("Checking braces, offset 222 [{}]", str::from_utf8(&line[..]).unwrap());
                    let message: Message = serde_json::from_slice(&line[..])?;
                    self.message = Some(message);
                }
            }
        }
        let mut got_payload = false;
        if let Some(message) = &self.message {
            let mut message = message.clone();
            if buf.len() >= message.payload_size {
                message.payload = buf[..message.payload_size].to_vec();
                buf.advance(message.payload_size);
                got_payload = true;
                result = Ok(Some(message));
            } else {
                result = Ok(None);
            }
        } else {
            result = Ok(None);
        }
        if got_payload {
            self.message = None;
        }
        result
    }
}

impl Encoder for InMessageCodec {
    type Item = String;
    type Error = io::Error;

    fn encode(&mut self, line: String, buf: &mut BytesMut) -> Result<(), io::Error> {
        buf.reserve(line.len() + 1);
        buf.put(line);
        buf.put_u8(b'\n');
        Ok(())
    }
}

/// A simple `Codec` implementation that reads incoming client topics.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ClientTopicsCodec {
    //topics: Option<ClientTopics>,
}

/*impl ClientTopicsCodec {
    pub fn new() -> ClientTopicsCodec {
        ClientTopicsCodec { topics: None }
    }
}*/

impl Decoder for ClientTopicsCodec {
    type Item = ClientIncoming;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<ClientIncoming>, io::Error> {
        let mut result: Result<Option<ClientIncoming>, io::Error> = Ok(None);
        if let Some(brace_offset) = buf[..].iter().position(|b| *b == b'{') {
            if brace_offset > 0 {
                buf.advance(brace_offset);
            }
        }
        if let Some(message_offset) = last_brace(&buf[..]) {
            if message_offset > 3 {
                let line = buf.split_to(message_offset + 1);
                let line_str:String;
                match str::from_utf8(&line[..]) {
                    Ok(str) => line_str = str.to_string(),
                    Err(err) => return Err(io::Error::new(io::ErrorKind::Other, err.description())),
                }
                if line_str.to_lowercase().contains("topics") {
                    let topics: ClientTopics = serde_json::from_slice(&line[..])?;
                    result = Ok(Some(ClientIncoming::Topic(topics)));
                } else {
                    let status: Status = serde_json::from_slice(&line[..])?;
                    result = Ok(Some(ClientIncoming::Status(status)));
                }
            }
        }
        result
    }
}

impl Encoder for ClientTopicsCodec {
    type Item = ClientMessage;
    type Error = io::Error;

    fn encode(&mut self, client_message: ClientMessage, buf: &mut BytesMut) -> Result<(), io::Error> {
        match client_message {
            ClientMessage::StatusOk => {
                let status = "{ \"status\": \"OK\"}".to_string();
                buf.reserve(status.len() + 1);
                buf.put(status);
                buf.put_u8(b'\n');
            },
            ClientMessage::StatusError(code, message) => {
                let status = format!("{{ \"status\": \"ERROR\", \"code\": {}, \"message\": \"{}\" }}", code, message);
                buf.reserve(status.len() + 1);
                buf.put(status);
                buf.put_u8(b'\n');
            },
            ClientMessage::Message(message) => {
                let json = format!("{{ \"topic\": \"{}\", \"payload_size\": {}, \"checksum\": \"{}\", \"sequence\": {} }}",
                                   message.topic, message.payload_size, message.checksum, message.sequence);
                buf.reserve(json.len() + message.payload_size + 1);
                buf.put(json);
                buf.put(message.payload);
                buf.put_u8(b'\n');
            },
            ClientMessage::Over => {}
        }
        Ok(())
    }
}

fn send_wait<T>(tx: &mut mpsc::Sender<T>, item: T) -> Result<(), &str> {
    match tx.start_send(item) {
        Ok(AsyncSink::Ready) => {
            match tx.poll_complete() {
                Ok(_) => Ok(()),
                Err(_) => Err("Poll complete failed, channel unusable!")
            }
        },
        Ok(AsyncSink::NotReady(item)) => {
            match tx.clone().send(item).wait() {
                Ok(_tx) => Ok(()),
                Err(_) => Err("Sending data failed, aborting!"),
            }
        },
        Err(_) => Err("Start send failed, channel unusable!")
    }
}

//fn send_writer<C: Sink, T>(writer: &mut stream::SplitSink<Framed<TcpStream, C>>, item: T) -> Result<(), &str> {
fn send_writer<C: Sink>(writer: &mut C, item: C::SinkItem) -> Result<(), &str> {
    match writer.start_send(item) {
        Ok(AsyncSink::Ready) => {
            match writer.poll_complete() {
                Ok(_) => Ok(()),
                Err(_) => Err("Poll complete failed, tcp sink unusable!")
            }
        },
        Ok(AsyncSink::NotReady(_)) => {
            Err("Tcp sink not ready for data, closing!")
        },
        Err(_) => Err("Start send failed, tcp sink unusable!")
    }
}

fn new_pub_server(tx_in: mpsc::Sender<BrokerMessage>) -> impl Future<Item=(), Error=()> {
    let addr_pub = "127.0.0.1:12345".parse().unwrap();
    let listener_pub = TcpListener::bind(&addr_pub)
        .expect("unable to bind pub TCP listener");

    // Pull out a stream of sockets for incoming connections
    let server_pub = listener_pub.incoming()
        .map_err(|e| eprintln!("accept failed = {:?}", e))
        .for_each(move |socket| {
            let mut tx = tx_in.clone();
            println!("Got PUB connection: {}", socket.peer_addr().unwrap());

            let framed = InMessageCodec::new().framed(socket);
            let (mut writer, reader) = framed.split();

            let processor = reader
                .for_each(move |message| {
                    if let Err(str) = send_writer(&mut writer, "{ \"status\": \"OK\"}".to_string()) {
                        return Err(io::Error::new(io::ErrorKind::Other, str));
                    }
                    if let Err(str) = send_wait(&mut tx, BrokerMessage::Message(message)) {
                        return Err(io::Error::new(io::ErrorKind::Other, str));
                    }
                    Ok(())
                })
                // After our copy operation is complete we just print out some helpful
                // information.
                .and_then(|()| {
                    println!("Socket received FIN packet and closed connection");
                    Ok(())
                })
                .or_else(|err| {
                    println!("Socket closed with error: {:?}", err);
                    // We have to return the error to catch it in the next ``.then` call
                    Err(err)
                })
                .then(|result| {
                    println!("Socket closed with result: {:?}", result);
                    Ok(())
                });

            tokio::spawn(processor)
        });

    server_pub
}

fn new_sub_server(tx_in: mpsc::Sender<BrokerMessage>) -> impl Future<Item=(), Error=()> {
    let addr_sub = "127.0.0.1:12346".parse().unwrap();
    let listener_sub = TcpListener::bind(&addr_sub)
        .expect("unable to bind sub TCP listener");

    // Pull out a stream of sockets for incoming connections
    let server_sub = listener_sub.incoming()
        .map_err(|e| eprintln!("accept failed = {:?}", e))
        .for_each(move |socket| {
            println!("Got SUB connection: {}", socket.peer_addr().unwrap());
            let mut tx = tx_in.clone();
            let (mut broker_tx, rx) = mpsc::channel(10);
            let mut loop_broker_tx = broker_tx.clone();

            let framed = ClientTopicsCodec{}.framed(socket);
            let (mut writer, reader) = framed.split();

            let processor = reader
                .for_each(move |incoming| {
                    match incoming {
                        ClientIncoming::Topic(topics) => {
                            if let Err(str) = send_wait(&mut tx, BrokerMessage::NewClient(topics, broker_tx.clone())) {
                                if let Err(_) = send_wait(&mut broker_tx, ClientMessage::StatusError(1, str.to_string())) {}
                                return Err(io::Error::new(io::ErrorKind::Other, str));
                            }
                            if let Err(str) = send_wait(&mut broker_tx, ClientMessage::StatusOk) {
                                return Err(io::Error::new(io::ErrorKind::Other, str));
                            }
                        },
                        ClientIncoming::Status(status) => {
                            if status.status.to_lowercase().eq("close") {
                                if let Err(_) = send_wait(&mut broker_tx, ClientMessage::Over) {}
                            }
                        },
                    }
                    Ok(())
                })
                // After our copy operation is complete we just print out some helpful
                // information.
                .and_then(|()| {
                    println!("Socket received FIN packet and closed connection");
                    Ok(())
                })
                .or_else(move |err| {
                    //let mut broker_tx = broker_tx.clone();
                    println!("Socket closed with error: {:?}", err);
                    if let Err(_) = send_wait(&mut loop_broker_tx, ClientMessage::StatusError(1, err.description().to_string())) {}
                    // We have to return the error to catch it in the next ``.then` call
                    Err(err)
                })
                .then(|result| {
                    println!("Socket closed with result: {:?}", result);
                    Ok(())
                });
            let processor2 = rx.for_each(move |client_message| {
                if let ClientMessage::Over = client_message {
                    return Err(());
                }
                let mut over = false;
                if let ClientMessage::StatusError(_, _) = client_message {
                    over = true;
                }
                if let Err(_) = send_writer(&mut writer, client_message) {
                    return Err(());
                }
                if over {
                    return Err(());
                }
                Ok(())
            });

            tokio::spawn(processor.join(processor2).and_then(|_x| Ok(())))
        });

    server_sub
}

fn new_message_broker(rx: mpsc::Receiver<BrokerMessage>) -> impl Future<Item=(), Error=()> {
    //let mut clients: Vec<mpsc::Sender<Message>> = vec![];
    let mut clients: HashMap<String, Vec<mpsc::Sender<ClientMessage>>> = HashMap::new();
    let mut sequences: HashMap<String, usize> = HashMap::new();
    rx.for_each(move |mes| {
        match mes {
            BrokerMessage::Message(mut message) => {
                // insert a key only if it doesn't already exist
                let seq = sequences.entry(message.topic.to_string()).or_insert(0);
                message.sequence = *seq;
                *seq += 1;
                //if clients.contains_key(&message.topic) {
                if let Some(mut txs) = clients.get_mut(&message.topic) {
                    txs.iter_mut().for_each(|tx| {
                        if let Err(_err) = send_wait(tx, ClientMessage::Message(message.clone())) {
                            // XXX remove the dead tx...
                        }
                    });
                }
            },
            BrokerMessage::NewClient(topics, tx) => {
                for topic in &topics.topics {
                    // insert a key only if it doesn't already exist
                    clients.entry(topic.to_string()).or_insert(Vec::with_capacity(20));
                    if let Some(mut txs) = clients.get_mut(topic) {
                        txs.push(tx.clone());
                    }
                }
            },
        };
        Ok(())
    })
}

fn main() {
    // Start the Tokio runtime
    let (tx, rx) = mpsc::channel(10);
    tokio::run(new_pub_server(tx.clone()).join3(new_sub_server(tx.clone()), new_message_broker(rx)).and_then(|_x| Ok(())));
}