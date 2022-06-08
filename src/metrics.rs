use hyper::header::CONTENT_TYPE;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use prometheus::{Encoder, TextEncoder, gather};
use std::net::SocketAddr;

async fn serve_req(_req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    let encoder = TextEncoder::new();

    let metric_families = gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();

    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap();

    Ok(response)
}

pub fn start_http_server(addr: SocketAddr) {
    std::thread::spawn(move || {
        let mut runtime = tokio::runtime::Builder::new_current_thread();
        runtime.enable_all();
        let runtime = runtime.build().unwrap();
        runtime
            .block_on(async move {
                Server::bind(&addr)
                    .serve(make_service_fn(|_| async {
                        Ok::<_, hyper::Error>(service_fn(serve_req))
                    }))
                    .await
            })
            .unwrap();
    });
}
