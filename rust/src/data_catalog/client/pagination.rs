//! Handle paginates results
use std::future::Future;

use futures::Stream;

use crate::data_catalog::DataCatalogResult;

/// Takes a paginated operation `op` that when called with:
///
/// - A state `S`
/// - An optional next token `Option<String>`
///
/// Returns
///
/// - A response value `T`
/// - The next state `S`
/// - The next continuation token `Option<String>`
///
/// And converts it into a `Stream<Result<T>>` which will first call `op(state, None)`, and yield
/// the returned response `T`. If the returned continuation token was `None` the stream will then
/// finish, otherwise it will continue to call `op(state, token)` with the values returned by the
/// previous call to `op`, until a continuation token of `None` is returned
///
pub fn stream_paginated<F, Fut, S, T>(state: S, op: F) -> impl Stream<Item = DataCatalogResult<T>>
where
    F: Fn(S, Option<String>) -> Fut + Copy,
    Fut: Future<Output = DataCatalogResult<(T, S, Option<String>)>>,
{
    enum PaginationState<T> {
        Start(T),
        HasMore(T, String),
        Done,
    }

    futures::stream::unfold(PaginationState::Start(state), move |state| async move {
        let (s, page_token) = match state {
            PaginationState::Start(s) => (s, None),
            PaginationState::HasMore(s, page_token) if !page_token.is_empty() => {
                (s, Some(page_token))
            }
            _ => {
                return None;
            }
        };

        let (resp, s, continuation) = match op(s, page_token).await {
            Ok(resp) => resp,
            Err(e) => return Some((Err(e), PaginationState::Done)),
        };

        let next_state = match continuation {
            Some(token) => PaginationState::HasMore(s, token),
            None => PaginationState::Done,
        };

        Some((Ok(resp), next_state))
    })
}
