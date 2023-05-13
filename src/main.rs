#![feature(async_closure)]
use actix_web::{web, App, HttpServer};
use clap::Parser;

mod replication;
mod routes;
mod store;
mod wal;

use replication::*;
use routes::*;
use store::*;
use wal::*;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    port: u16,
    #[arg(short, long, default_value = "")]
    follow: String,
    #[arg(short, long, default_value_t = false)]
    asynchronous: bool,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let async_replicas = web::Data::new(AsyncReplicas::new().await);
    let http_server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(Log::new()))
            .app_data(web::Data::new(Store::new()))
            .app_data(async_replicas.clone())
            .app_data(web::Data::new(SyncReplicas::new()))
            .service(get)
            .service(put)
            .service(replica)
    });

    println!("Listening on port {}", args.port);
    let server = http_server.bind(("127.0.0.1", args.port)).unwrap().run();

    if args.follow != "" {
        let resp = reqwest::Client::new()
            .put(format!("{}/replica", args.follow))
            .json(&Replica::new(args.port, args.asynchronous))
            .send()
            .await;
        match resp {
            Ok(resp) => println!("Registered with primary: {}", resp.status()),
            Err(e) => {
                return std::io::Result::Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to register with primary: {}", e),
                ))
            }
        }
    }

    server.await
}
