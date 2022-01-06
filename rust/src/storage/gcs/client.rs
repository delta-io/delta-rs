use super::{util, GCSClientError, GCSObject};
use futures::Stream;
use std::convert::{TryFrom, TryInto};
use std::path::PathBuf;
/// Google Cloud Storage http client
use std::sync::Arc;
use tame_gcs::objects::{self, Object};
use tame_oauth::gcp as oauth;

use log::debug;

/// Struct maintaining the state responsible for communicating
/// with the google cloud storage service
pub struct GCSStorageBackend {
    /// The reqwest client used for handling http requests
    pub client: reqwest::Client,
    /// The path to the path to the credentials file
    pub cred_path: PathBuf,
    /// The handle to our oauth token
    pub auth: Arc<oauth::ServiceAccountAccess>,
}

impl std::fmt::Debug for GCSStorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("GCSStorageBackend {...}").finish()
    }
}

impl TryFrom<PathBuf> for GCSStorageBackend {
    type Error = GCSClientError;
    fn try_from(cred_path: PathBuf) -> Result<Self, Self::Error> {
        let client = reqwest::Client::builder().build()?;
        let cred_contents = std::fs::read_to_string(&cred_path)
            .map_err(|source| Self::Error::CredentialsError { source })?;
        let svc_account_info = oauth::ServiceAccountInfo::deserialize(cred_contents)?;
        let svc_account_access = oauth::ServiceAccountAccess::new(svc_account_info)?;

        Ok(Self {
            client,
            cred_path,
            auth: std::sync::Arc::new(svc_account_access),
        })
    }
}

impl GCSStorageBackend {
    pub async fn metadata<'a>(
        &self,
        path: GCSObject<'_>,
    ) -> Result<objects::Metadata, GCSClientError> {
        debug!("creating request");
        let get_meta_request = Object::get(&path, None)?;
        debug!("executing request");
        let response =
            util::execute::<_, objects::GetObjectResponse>(self, get_meta_request).await?;

        debug!("returning meta");
        Ok(response.metadata)
    }

    pub async fn download<'a>(&self, path: GCSObject<'_>) -> Result<bytes::Bytes, GCSClientError> {
        let download_request = Object::download(&path, None)?;

        let response = util::execute::<_, objects::DownloadObjectResponse>(self, download_request)
            .await
            .map_err(util::check_object_not_found)?;

        Ok(response.consume())
    }

    pub fn list<'a>(
        &'a self,
        uri: GCSObject<'a>,
    ) -> impl Stream<Item = Result<objects::Metadata, GCSClientError>> + 'a {
        let mut page_token: Option<String> = None;

        async_stream::try_stream! {
            loop {

                let list_request_opts = Some(objects::ListOptional {
                    prefix: Some(uri.path.as_ref()),
                    page_token: page_token.as_deref(),
                    standard_params: tame_gcs::common::StandardQueryParameters {
                        // We are only interested in the name and updated timestamp
                        // to subsequently populate the ObjectMeta struct
                        fields: Some("items(name, updated"),
                        ..Default::default()
                    },
                    ..Default::default()
                });

                let list_request = Object::list(&uri.bucket, list_request_opts)?;
                let list_response = util::execute::<_, objects::ListResponse>(
                    self, list_request).await?;

                for object_meta in list_response.objects {
                    yield object_meta
                }

                // If we have a page token it means there may be more items
                // that fulfill the parameters
                page_token = list_response.page_token;
                if page_token.is_none() {
                    break;
                }
            }
        }
    }

    pub async fn insert<'a, 'b>(
        &self,
        uri: GCSObject<'a>,
        content: Vec<u8>,
    ) -> Result<(), GCSClientError> {
        let content_len = content.len().try_into().unwrap();
        let content_body = std::io::Cursor::new(content);

        let insert_request = Object::insert_simple(&uri, content_body, content_len, None)?;
        let _response = util::execute::<_, objects::InsertResponse>(self, insert_request).await?;

        Ok(())
    }

    pub async fn rename_noreplace<'a>(
        &self,
        src: GCSObject<'a>,
        dst: GCSObject<'a>,
    ) -> Result<(), GCSClientError> {
        let mut rewrite_token = None;

        loop {
            let metadata = None;
            let precondition = Some(objects::RewriteObjectOptional {
                destination_conditionals: Some(tame_gcs::common::Conditionals {
                    if_generation_match: Some(0),
                    ..Default::default()
                }),
                ..Default::default()
            });

            let rewrite_http_request =
                Object::rewrite(&src, &dst, rewrite_token, metadata, precondition)?;
            let response =
                util::execute::<_, objects::RewriteObjectResponse>(self, rewrite_http_request)
                    .await
                    .map_err(util::check_precondition_status)?;

            rewrite_token = response.rewrite_token;
            if rewrite_token.is_none() {
                break;
            }
        }

        self.delete(src).await
    }

    pub async fn delete<'a>(&self, uri: GCSObject<'_>) -> Result<(), GCSClientError> {
        let delete_request = Object::delete(&uri, None)?;
        let _response =
            util::execute::<_, objects::DeleteObjectResponse>(self, delete_request).await?;
        Ok(())
    }
}
