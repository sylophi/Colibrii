//! What runs while the device is enrolled.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::device::Inner;
use crate::errors::Error;
use crate::store::Enrollment;

pub(crate) struct Session {
    ended: CancellationToken,
}

impl Session {
    pub async fn start(_inner: Arc<Inner>, _enrollment: Enrollment) -> Result<Arc<Session>, Error> {
        Ok(Arc::new(Session { ended: CancellationToken::new() }))
    }

    pub async fn end(&self) {
        self.ended.cancel();
    }
}
