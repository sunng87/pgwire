use futures_util::future::{AbortHandle, Abortable};
use std::io;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{self, Duration};

/// A test that starts a TCP server, runs it for 20 seconds, and then shuts it down gracefully.
/// Each incoming connection has a 10-second read timeout.
#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create abort handle for graceful shutdown
    let (abort_handle, abort_registration) = AbortHandle::new_pair();

    // Clone the abort handle for the shutdown task
    let shutdown_handle = abort_handle.clone();

    // Start the server in a separate task
    let server_task = tokio::spawn(async move { run_server(abort_registration).await });

    // Start a task to shutdown the server after 20 seconds
    let shutdown_task = tokio::spawn(async move {
        time::sleep(Duration::from_secs(20)).await;
        println!("Shutting down server after 20 seconds");
        shutdown_handle.abort();
    });

    time::sleep(Duration::from_millis(100)).await;

    let _ = tokio::spawn(async move {
        time::sleep(Duration::from_secs(1)).await; // Wait for server to be ready

        if let Ok(mut stream) = TcpStream::connect("127.0.0.1:12306").await {
            println!("Client connected");
            time::sleep(Duration::from_secs(20)).await;
            // stream.write_all(b"Hello, server!\n").await.ok();

            // let mut buf = [0u8; 1024];
            // if let Ok(n) = stream.read(&mut buf).await {
            //     let response = String::from_utf8_lossy(&buf[..n]);
            //     println!("Client received: {}", response);
            // }
        }
    });

    // Wait for either the server to complete or the shutdown to trigger
    tokio::select! {
        result = server_task => {
            match result {
                Ok(Ok(())) => println!("Server task completed successfully"),
                Ok(Err(e)) => println!("Server task failed: {}", e),
                Err(e) => println!("Server task panicked: {}", e),
            }
        }
        _ = shutdown_task => {
            println!("Shutdown task completed");
        }
    }

    println!("Test completed");
    Ok(())
}

async fn run_server(
    abort_registration: futures_util::future::AbortRegistration,
) -> Result<(), io::Error> {
    let listener = TcpListener::bind("127.0.0.1:12306").await?;
    let addr = listener.local_addr()?;
    println!("Server listening on {}", addr);

    // Wrap the listener's accept loop with Abortable
    let server_future = async move {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    println!("New connection from {}", addr);
                    // Spawn a task to handle each connection
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream).await {
                            println!("Error handling connection: {}", e);
                        }
                    });
                }
                Err(e) => {
                    println!("Error accepting connection: {}", e);
                    break;
                }
            }
        }
    };

    let abortable_server = Abortable::new(server_future, abort_registration);

    match abortable_server.await {
        Ok(()) => println!("Server completed normally"),
        Err(_) => println!("Server was aborted gracefully"),
    }

    Ok(())
}

async fn handle_connection(mut stream: TcpStream) -> Result<(), io::Error> {
    let (reader, mut writer) = stream.split();
    let mut buf_reader = BufReader::new(reader);
    let mut line = String::new();

    // Use tokio::select! to read with a 10-second timeout
    tokio::select! {
        result = buf_reader.read_line(&mut line) => {
            match result {
                Ok(0) => {
                    println!("Connection closed by client");
                }
                Ok(n) => {
                    println!("Received {} bytes: {}", n, line.trim());
                    // Echo back the received data
                    writer.write_all(format!("Echo: {}", line).as_bytes()).await?;
                    writer.flush().await?;
                }
                Err(e) => {
                    println!("Error reading from connection: {}", e);
                }
            }
        }
        _ = time::sleep(Duration::from_secs(30)) => {
            println!("Connection timed out after 30 seconds");
            writer.write_all(b"Timeout\n").await.ok();
            writer.flush().await.ok();
        }
    }

    Ok(())
}
