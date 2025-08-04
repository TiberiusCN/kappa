use tokio::{sync::oneshot::error::RecvError, time::error::Elapsed};

mod actor_system;

pub use crate::actor_system::*;
pub use tokio;
#[cfg(feature = "warp")]
pub use warp;

/// The shortcut to create a binary.
pub async fn init<F, Fut>(setup: F)
where
  F: FnOnce(ActorContext<Main>) -> Fut + Send + 'static,
  Fut: Future<Output = ActorContext<Main>> + Send + 'static,
{
  env_logger::init();
  ActorContext::start_system(setup).await;
  tokio::signal::ctrl_c().await.ok();
}

mod message {
  use super::*;
  pub enum Message {
    Signal(Signal),
  }
  message!();
}
pub use message::Message as Main;

#[derive(Debug, thiserror::Error)]
pub enum Error {
  #[error("No response listener found: {0}")]
  DeadRequest(#[from] RecvError),

  #[error("The request timed out: {0}")]
  Timeout(#[from] Elapsed),

  #[error("{0}")]
  Custom(String, &'static str),
}
pub trait CustomError: std::error::Error {
  fn code(&self) -> &'static str { "unknown" }
}
impl<J: CustomError> From<J> for Error {
  fn from(j: J) -> Self {
    Self::Custom(format!("{j}"), j.code())
  }
}
pub type Result<T> = std::result::Result<T, Error>;

/// Implements Reject for Error.
#[cfg(feature = "warp")]
impl warp::reject::Reject for Error {}
