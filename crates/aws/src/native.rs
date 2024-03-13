use aws_sdk_sts::config::SharedHttpClient;
use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;

pub fn use_native_tls_client(allow_http: bool) -> SharedHttpClient {
    let mut tls_connector = hyper_tls::HttpsConnector::new();
    if allow_http {
        tls_connector.https_only(false);
    }

    let client = HyperClientBuilder::new().build(tls_connector);
    client
}
