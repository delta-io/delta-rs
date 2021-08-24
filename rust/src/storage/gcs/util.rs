use super::{GCSClientError, GCSStorageBackend};
/// This code is largely duplicated from https://github.com/EmbarkStudios/gsutil
use bytes::BufMut;
use futures::StreamExt;
use std::convert::TryInto;
use std::iter::Iterator;
use tame_gcs::http;
use tame_oauth::gcp as oauth;

async fn get_token(backend: &GCSStorageBackend) -> Result<tame_oauth::Token, GCSClientError> {
    Ok(
        match backend.auth.get_token(&[tame_gcs::Scopes::ReadWrite])? {
            oauth::TokenOrRequest::Token(token) => token,
            oauth::TokenOrRequest::Request {
                request,
                scope_hash,
                ..
            } => {
                let (parts, body) = request.into_parts();
                let read_body = std::io::Cursor::new(body);
                let new_request = http::Request::from_parts(parts, read_body);

                let req = convert_request(new_request, &backend.client).await?;
                let res = backend.client.execute(req).await?;
                let response = convert_response(res).await?;
                backend.auth.parse_token_response(scope_hash, response)?
            }
        },
    )
}

/// Converts a vanilla `http::Request` into a `reqwest::Request`
async fn convert_request<B>(
    req: http::Request<B>,
    client: &reqwest::Client,
) -> Result<reqwest::Request, GCSClientError>
where
    B: std::io::Read + Send + 'static,
{
    let (parts, mut body) = req.into_parts();

    let uri = parts.uri.to_string();

    let builder = match parts.method {
        http::Method::GET => client.get(&uri),
        http::Method::POST => client.post(&uri),
        http::Method::DELETE => client.delete(&uri),
        http::Method::PATCH => client.patch(&uri),
        http::Method::PUT => client.put(&uri),
        method => panic!("Invalid http method: {}", method),
    };

    let content_len = tame_gcs::util::get_content_length(&parts.headers).unwrap_or(0);
    let mut buffer = bytes::BytesMut::with_capacity(content_len);

    let mut block = [0u8; 8 * 1024];

    loop {
        let read = body.read(&mut block)?;

        if read > 0 {
            buffer.extend_from_slice(&block[..read]);
        } else {
            break;
        }
    }

    Ok(builder
        .header(reqwest::header::CONTENT_LENGTH, content_len)
        .headers(parts.headers)
        .body(buffer.freeze())
        .build()?)
}

/// Converts a `reqwest::Response` into a vanilla `http::Response`. This currently copies
/// the entire response body into a single buffer with no streaming
async fn convert_response(
    res: reqwest::Response,
) -> Result<http::Response<bytes::Bytes>, GCSClientError> {
    let mut builder = http::Response::builder()
        .status(res.status())
        .version(res.version());

    let headers = builder
        .headers_mut()
        .ok_or_else(|| GCSClientError::Other("failed to convert response headers".to_string()))?;

    headers.extend(
        res.headers()
            .into_iter()
            .map(|(k, v)| (k.clone(), v.clone())),
    );

    let content_len = tame_gcs::util::get_content_length(headers).unwrap_or_default();
    let mut buffer = bytes::BytesMut::with_capacity(content_len);

    let mut stream = res.bytes_stream();

    while let Some(item) = stream.next().await {
        buffer.put(item?);
    }

    Ok(builder.body(buffer.freeze())?)
}

/// Executes a GCS request via a reqwest client and returns the parsed response/API error
pub async fn execute<B, R>(
    ctx: &GCSStorageBackend,
    mut req: http::Request<B>,
) -> Result<R, GCSClientError>
where
    R: tame_gcs::ApiResponse<bytes::Bytes>,
    B: std::io::Read + Send + 'static,
{
    // First, get our oauth token, which can mean we have to do an additional
    // request if we've never retrieved one yet, or the one we are using has expired
    let token = get_token(ctx).await?;

    // Add the authorization token, note that the tame-oauth crate will automatically
    // set the HeaderValue correctly, in the GCP case this is usually "Bearer <token>"
    req.headers_mut()
        .insert(http::header::AUTHORIZATION, token.try_into()?);

    let request = convert_request(req, &ctx.client).await?;
    let response = ctx.client.execute(request).await?;
    let response = convert_response(response).await?;

    Ok(R::try_from_parts(response)?)
}

use http::status::StatusCode;
use tame_gcs::error::HttpStatusError;
pub fn check_object_not_found(err: GCSClientError) -> GCSClientError {
    match err {
        GCSClientError::GCSError {
            source: tame_gcs::error::Error::HttpStatus(HttpStatusError(StatusCode::NOT_FOUND)),
        } => GCSClientError::NotFound,
        err => err,
    }
}

pub fn check_precondition_status(err: GCSClientError) -> GCSClientError {
    match err {
        GCSClientError::GCSError {
            source:
                tame_gcs::error::Error::HttpStatus(HttpStatusError(StatusCode::PRECONDITION_FAILED)),
        } => GCSClientError::PreconditionFailed,
        err => err,
    }
}
