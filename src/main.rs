#![allow(unused_imports)]
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    fn handle_connection(stream: &mut TcpStream) {
        loop {
            let mut buf = [0; 512];
            let buffer_size = stream.read(&mut buf).unwrap();
            if buffer_size == 0 {
                break;
            }
            stream.write(b"+PONG\r\n").unwrap();
        }
    }

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let handle = thread::spawn(move || handle_connection(&mut stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
