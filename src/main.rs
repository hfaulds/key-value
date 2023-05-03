use actix_web::{web, App, HttpServer};
use clap::Parser;

mod replication;
mod store;

use replication::*;
use store::*;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    port: u16,
    #[arg(short, long, default_value = "")]
    follow: String,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let store = web::Data::new(Store::new());
    let replicas = web::Data::new(Replicas::new());

    let http_server = HttpServer::new(move || {
        App::new()
            .app_data(store.clone())
            .app_data(replicas.clone())
            .service(get)
            .service(put)
            .service(replica)
    });

    println!("Listening on port {}", args.port);
    let server = http_server.bind(("127.0.0.1", args.port)).unwrap().run();

    if args.follow != "" {
        let resp = reqwest::Client::new()
            .put(format!("{}/replica", args.follow))
            .json(&Replica::new(args.port))
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
