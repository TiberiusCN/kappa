use crate::{Error, Result};
use std::{
  collections::HashMap,
  time::{Duration, Instant},
};
use tokio::{
  sync::{
    mpsc::{UnboundedReceiver, UnboundedSender, WeakUnboundedSender, unbounded_channel},
    oneshot::{Receiver, Sender, channel},
  },
  task::JoinHandle,
};

tokio::task_local! {
  static CONTEXT: String;
}

fn spawn_context<F, J>(name: J, j: F) -> JoinHandle<F::Output>
where
  F: Future + Send + 'static,
  F::Output: Send + 'static,
  J: FnOnce() -> String,
{
  #[cfg(feature = "actor-debug")]
  {
    tokio::spawn(CONTEXT.scope(name(), j))
  }
  #[cfg(not(feature = "actor-debug"))]
  {
    let _ = name;
    tokio::spawn(j)
  }
}

/// Wraps tokio's spawn function with te actor system context. This function should be used instead of tokio's spawn.
pub fn spawn<F>(j: F) -> JoinHandle<F::Output>
where
  F: Future + Send + 'static,
  F::Output: Send + 'static,
{
  let name = || CONTEXT.try_with(|ctx| format!("{ctx}.out")).unwrap_or_else(|_| "unknown_out".to_string());
  spawn_context(name, j)
}

/// The write half of the actor's channel.
/// Short-lived references can be cloned only into ignore-kind,
/// any other reference will be cloned as a strong reference.
pub enum ActorRef<J: Send> {
  /// Represents a temporary reference used for the ask-pattern.
  Ask {
    tx: Option<Sender<J>>,
    #[cfg(feature = "actor-debug")]
    name: String,
  },
  /// A long-lived strong reference.
  Strong {
    tx: UnboundedSender<J>,
    #[cfg(feature = "actor-debug")]
    name: String,
  },
  /// A weak reference that allows the corresponding actor to be dropped.
  Weak {
    tx: WeakUnboundedSender<J>,
    #[cfg(feature = "actor-debug")]
    name: String,
  },
}
impl<J: Send> Default for ActorRef<J> {
  fn default() -> Self {
    Self::Ask {
      tx: None,
      #[cfg(feature = "actor-debug")]
      name: "ignore".into(),
    }
  }
}
impl<J: Send> ActorRef<J> {
  /// A default reference simply drops the incoming messages.
  pub fn ignore() -> Self {
    Self::default()
  }
  #[doc(hidden)]
  pub fn ignore_named(name: String) -> Self {
    #[cfg(not(feature = "actor-debug"))]
    let _ = name;
    Self::Ask {
      tx: None,
      #[cfg(feature = "actor-debug")]
      name,
    }
  }

  /// Sends the message to the actor.
  pub fn tell(&mut self, j: J) {
    match self {
      Self::Ask {
        tx,
        #[cfg(feature = "actor-debug")]
        name,
      } => {
        #[cfg(feature = "actor-debug")]
        println!("{} ~> {name}", CONTEXT.try_with(|ctx| ctx.clone()).unwrap_or_else(|_| "unknown".into()));
        if let Some(f) = tx.take() {
          f.send(j).ok();
        }
      }
      Self::Strong {
        tx,
        #[cfg(feature = "actor-debug")]
        name,
      } => {
        #[cfg(feature = "actor-debug")]
        println!("{} => {name}", CONTEXT.try_with(|ctx| ctx.clone()).unwrap_or_else(|_| "unknown".into()));
        tx.send(j).ok();
      }
      Self::Weak {
        tx,
        #[cfg(feature = "actor-debug")]
        name,
      } => {
        #[cfg(feature = "actor-debug")]
        println!("{} -> {name}", CONTEXT.try_with(|ctx| ctx.clone()).unwrap_or_else(|_| "unknown".into()));
        if let Some(f) = tx.upgrade() {
          f.send(j).ok();
        }
      }
    }
  }

  /// Checks that the actor coresponding to the refeence is still alive.
  pub fn is_closed(&mut self) -> bool {
    match self {
      Self::Ask {
        tx: None,
        #[cfg(feature = "actor-debug")]
          name: _,
      } => true,
      Self::Ask {
        tx: Some(j),
        #[cfg(feature = "actor-debug")]
          name: _,
      } => j.is_closed(),
      Self::Strong {
        tx,
        #[cfg(feature = "actor-debug")]
          name: _,
      } => tx.is_closed(),
      Self::Weak {
        tx,
        #[cfg(feature = "actor-debug")]
          name: _,
      } => tx.upgrade().is_some(),
    }
  }

  /// Creates a short-lived reference, inserts it into the message and waits for the response.
  /// This can be used outside of the actor.
  pub async fn ask<T, F>(&mut self, f: F) -> Result<T>
  where
    T: Send + 'static,
    F: FnOnce(ActorRef<T>) -> J + Send,
  {
    Ok(self.ask_core(f).await?)
  }

  /// Same as ask(), but additionaly converts an error.
  /// This is usefull for messages that require ActorRef<Result<_, _>>.
  pub async fn ask_with_status<T, E, F>(&mut self, f: F) -> Result<T>
  where
    T: Send + 'static,
    E: Into<Error> + Send + 'static,
    F: FnOnce(ActorRef<std::result::Result<T, E>>) -> J + Send,
  {
    self.ask_core(f).await?.map_err(Into::into)
  }

  #[doc(hidden)]
  pub async fn ask_map<T, F, K, L>(&mut self, f: F, mut to: ActorRef<Result<K>>, l: L)
  where
    T: Send + 'static,
    K: Send + 'static,
    F: FnOnce(ActorRef<T>) -> J + Send,
    L: FnOnce(T) -> K + Send + 'static,
  {
    let (tx, rx) = channel::<T>();
    self.tell(f(tx.into()));
    spawn(async move {
      to.tell(rx.await.map(l).map_err(Into::into));
    });
  }

  #[doc(hidden)]
  pub async fn ask_map_with_status<T, E, F, K, L>(&mut self, f: F, mut to: ActorRef<Result<K>>, l: L)
  where
    T: Send + 'static,
    E: Into<Error> + Send + 'static,
    K: Send + 'static,
    F: FnOnce(ActorRef<std::result::Result<T, E>>) -> J + Send,
    L: FnOnce(T) -> K + Send + 'static,
  {
    let (tx, rx) = channel::<std::result::Result<T, E>>();
    self.tell(f(tx.into()));
    spawn(async move {
      let j: Result<_> = rx.await.map_err(Into::into);
      let j = match j {
        Ok(Ok(j)) => Ok(j),
        Ok(Err(j)) => Err(j.into()),
        Err(j) => Err(j),
      };
      to.tell(j.map(l));
    });
  }

  /// Resends an ask message.
  pub async fn ask_pipe<T, F>(&mut self, f: F, to: ActorRef<Result<T>>)
  where
    T: Send + 'static,
    F: FnOnce(ActorRef<T>) -> J + Send,
  {
    self.ask_map(f, to, |j| j).await
  }

  /// Resends an ask-status message.
  pub async fn ask_pipe_with_status<T, E, F>(&mut self, f: F, to: ActorRef<Result<T>>)
  where
    T: Send + 'static,
    E: Into<Error> + Send + 'static,
    F: FnOnce(ActorRef<std::result::Result<T, E>>) -> J + Send,
  {
    self.ask_map_with_status(f, to, |j| j).await
  }

  /// Makes the weak reference.
  pub fn weak(&self) -> Self {
    match self {
      Self::Ask {
        tx: _,
        #[cfg(feature = "actor-debug")]
        name,
      } => Self::Ask {
        tx: None,
        #[cfg(feature = "actor-debug")]
        name: name.clone(),
      },
      Self::Strong {
        tx,
        #[cfg(feature = "actor-debug")]
        name,
      } => Self::Weak {
        tx: tx.downgrade(),
        #[cfg(feature = "actor-debug")]
        name: name.clone(),
      },
      Self::Weak {
        tx,
        #[cfg(feature = "actor-debug")]
        name,
      } => Self::Weak {
        tx: tx.clone(),
        #[cfg(feature = "actor-debug")]
        name: name.clone(),
      },
    }
  }

  fn ask_core<T, F>(&mut self, f: F) -> Receiver<T>
  where
    T: Send,
    F: FnOnce(ActorRef<T>) -> J + Send,
  {
    let (tx, rx) = channel::<T>();
    self.tell(f(tx.into()));
    rx
  }

  fn same(&self, other: &Self) -> bool {
    match (self, other) {
      (Self::Strong { tx: f, .. }, Self::Weak { tx: j, .. }) => {
        j.upgrade().map(|j| j.same_channel(f)).unwrap_or_default()
      }
      (Self::Weak { tx: j, .. }, Self::Strong { tx: f, .. }) => {
        j.upgrade().map(|j| j.same_channel(f)).unwrap_or_default()
      }
      (Self::Strong { tx: f, .. }, Self::Strong { tx: j, .. }) => j.same_channel(f),
      (Self::Weak { tx: f, .. }, Self::Weak { tx: j, .. }) => match (f.upgrade(), j.upgrade()) {
        (Some(f), Some(j)) => j.same_channel(&f),
        _ => false,
      },
      _ => false,
    }
  }
}
impl<J: Send> PartialEq for ActorRef<J> {
  fn eq(&self, other: &Self) -> bool {
    self.same(other)
  }
}
impl<J: Send> Eq for ActorRef<J> {}
impl<J: Send> Clone for ActorRef<J> {
  fn clone(&self) -> Self {
    match self {
      Self::Ask {
        #[cfg(feature = "actor-debug")]
        name,
        ..
      } => {
        #[cfg(feature = "actor-debug")]
        {
          Self::ignore_named(name.clone())
        }
        #[cfg(not(feature = "actor-debug"))]
        {
          Self::ignore()
        }
      }
      Self::Strong {
        tx,
        #[cfg(feature = "actor-debug")]
        name,
      } => Self::Strong {
        tx: tx.clone(),
        #[cfg(feature = "actor-debug")]
        name: name.clone(),
      },
      Self::Weak {
        tx,
        #[cfg(feature = "actor-debug")]
        name,
      } => {
        if let Some(tx) = tx.upgrade() {
          Self::Strong {
            tx,
            #[cfg(feature = "actor-debug")]
            name: name.clone(),
          }
        } else {
          #[cfg(feature = "actor-debug")]
          {
            Self::ignore_named(name.clone())
          }
          #[cfg(not(feature = "actor-debug"))]
          {
            Self::ignore()
          }
        }
      }
    }
  }
}
impl<J: Send> From<Sender<J>> for ActorRef<J> {
  fn from(j: Sender<J>) -> Self {
    Self::Ask {
      tx: Some(j),
      #[cfg(feature = "actor-debug")]
      name: "raw".into(),
    }
  }
}
impl<J: Send, F: Into<String>> From<(UnboundedSender<J>, F)> for ActorRef<J> {
  fn from(tx: (UnboundedSender<J>, F)) -> Self {
    Self::Strong {
      tx: tx.0,
      #[cfg(feature = "actor-debug")]
      name: tx.1.into(),
    }
  }
}

/// Common message protocol.
#[derive(Debug)]
pub enum Signal {
  Stop,
  Unchild(String),
}
/// The message trait.
pub trait Signaled: 'static + Send + From<Signal> + TryInto<Signal, Error = Self> {}
impl<J: 'static + Send + From<Signal> + TryInto<Signal, Error = Self>> Signaled for J {}

/// An actor. An actor system is technically the root actor.
pub struct ActorContext<J: Signaled> {
  rx: UnboundedReceiver<J>,
  tx: ActorRef<J>,
  children: HashMap<String, (ActorRef<Signal>, Option<Box<dyn FnOnce() + Send>>)>,
  tick: Instant,
  name: String,
}
impl<J: Signaled> ActorContext<J> {
  /// Used primarily for debug output.
  pub fn name(&self) -> &str {
    &self.name
  }

  /// Runs the root actor inside the tokio runtime.
  /// All the requirements for a child actor apply here as well.
  pub async fn start_system<F, Fut>(setup: F) -> ActorRef<J>
  where
    F: FnOnce(ActorContext<J>) -> Fut + Send + 'static,
    Fut: Future<Output = Self> + Send + 'static,
  {
    CONTEXT
      .scope("root".into(), async move {
        let (tx, rx) = unbounded_channel::<J>();
        let tick = Instant::now();
        let tx = ActorRef::Strong {
          tx: tx.clone(),
          #[cfg(feature = "actor-debug")]
          name: "root".into(),
        };
        let ctx = ActorContext { rx, tx, children: Default::default(), tick, name: "root".into() };
        let out = ctx.tx();
        spawn_context(|| "root".into(), setup(ctx));
        out
      })
      .await
  }

  /// Spawns a child actor.
  /// The child actor will receive Signal::Stop when the parent actor becomes dead.
  /// Furthermore, the child actor will stop its working cycle by closing the receiver.
  /// You can create an on-kill callback by running code after the receiving cycle.
  /// The parent actor holds a weak reference to the newly created child actor.
  /// You must provide an asynchronous routine that receives messages in a loop.
  /// If you return the context from the setup function, the actor will be closed.
  /// You can also use some additional child-specific functions within SpawnContext.
  pub async fn spawn<'j, T, F, Fut, Name>(&'j mut self, name: Name, setup: F) -> SpawnContext<'j, J, T>
  where
    T: Signaled,
    F: FnOnce(ActorContext<T>) -> Fut + Send + 'static,
    Fut: Future<Output = ActorContext<T>> + Send + 'static,
    Name: AsRef<str>,
  {
    let (tx, rx) = unbounded_channel::<T>();
    let tick = self.tick.clone();
    let (stx, mut srx) = unbounded_channel::<Signal>();
    let name = name.as_ref();
    let name = loop {
      let id = self.tick.elapsed().as_millis();
      let name = format!("{name}_{id}");
      if !self.children.contains_key(&name) {
        break name;
      }
    };
    let stx = ActorRef::Strong {
      tx: stx,
      #[cfg(feature = "actor-debug")]
      name: name.clone(),
    };
    let mut otx = ActorRef::Weak {
      tx: tx.downgrade(),
      #[cfg(feature = "actor-debug")]
      name: name.clone(),
    };
    let ctx = ActorContext {
      rx,
      tx: ActorRef::Strong {
        tx,
        #[cfg(feature = "actor-debug")]
        name: name.clone(),
      },
      children: Default::default(),
      tick,
      name: name.clone(),
    };
    #[allow(unused_mut)]
    let child = name.clone();
    let mut me = self.tx.clone();
    spawn(async move {
      while let Some(j) = srx.recv().await {
        otx.tell(j.into());
      }
      me.tell(Signal::Unchild(name).into());
    });
    let name = child.clone();
    self.children.insert(child, (stx.weak(), None));
    let out = ctx.tx();
    #[cfg(feature = "actor-debug")]
    println!("{name} @ {}", self.name);
    spawn_context(|| name.clone(), async move {
      let mut stx = stx;
      let mut ctx = setup(ctx).await;
      stx.tell(Signal::Stop);
      while let Some(_) = ctx.recv().await {}
    });
    SpawnContext { parent: self, child: out, name }
  }

  /// Converts the loop cycle into a weak version.
  /// The actor will receive a Signal::Stop if there are no more strong references.
  pub fn into_weak(mut self) -> Self {
    self.tx = self.tx.weak();
    self
  }

  /// Creates a strong actor reference.
  pub fn tx(&self) -> ActorRef<J> {
    self.tx.clone()
  }

  /// Receives messages. You must do this in loop of the long-lived actors.
  pub async fn recv(&mut self) -> Option<J> {
    while let Some(j) = self.rx.recv().await {
      match j.try_into() {
        Ok(Signal::Stop) => {
          self.rx.close();
        }
        Ok(Signal::Unchild(name)) => {
          if let Some(j) = self.children.remove(&name).and_then(|mut j| j.1.take()) {
            j()
          }
        }
        Err(j) => {
          return Some(j);
        }
      }
    }
    None
  }

  /// Sends a message to the actor itself.
  pub fn tell(&mut self, j: J) {
    self.tx.tell(j)
  }

  /// Builds a short-lived actor that awaits a result and sends it as a message to itself.
  /// Note that a message can contain multiple response references, allowing you to await a response
  /// from any of the ask actors.
  pub async fn ask<A, T, F1, F2>(&mut self, tx: &ActorRef<A>, f: F1, on_result: F2)
  where
    A: Send + 'static,
    T: Send + 'static,
    F1: FnOnce(ActorRef<T>) -> A + Send + 'static,
    F2: FnOnce(Result<T>) -> Vec<J> + Send + 'static,
  {
    let mut me = self.tx.clone();
    let mut tx = tx.clone();
    spawn(async move {
      for j in on_result(tx.ask(f).await) {
        me.tell(j);
      }
    });
  }

  /// Same as ask(), but additionaly converts an error.
  pub async fn ask_with_status<A, T, E, F1, F2>(&mut self, tx: &ActorRef<A>, f: F1, on_result: F2)
  where
    A: Send + 'static,
    T: Send + 'static,
    E: Into<Error> + Send + 'static,
    F1: FnOnce(ActorRef<std::result::Result<T, E>>) -> A + Send + 'static,
    F2: FnOnce(Result<T>) -> Vec<J> + Send + 'static,
  {
    self
      .ask(tx, f, |j| {
        on_result(match j {
          Ok(Ok(j)) => Ok(j),
          Ok(Err(j)) => Err(j.into()),
          Err(j) => Err(j),
        })
      })
      .await;
  }

  /// Adds a timer reference to the actor loop.
  /// This reference allows you to send any message to itself after a specific delay.
  pub async fn with_timers<Fut, F>(self, f: F) -> Self
  where
    Fut: Future<Output = Self>,
    F: FnOnce(Self, ActorRef<(Duration, J)>) -> Fut,
  {
    let (tx, mut rx) = unbounded_channel();
    #[cfg(feature = "actor-debug")]
    let name = format!("{}.timers", self.name);
    let tx = ActorRef::Strong {
      tx,
      #[cfg(feature = "actor-debug")]
      name: name.clone(),
    };
    let me = self.tx();
    let inner = async move {
      while let Some((timeout, j)) = rx.recv().await {
        let mut me = me.clone();
        spawn(async move {
          tokio::time::sleep(timeout).await;
          me.tell(j);
        });
      }
    };
    #[cfg(feature = "actor-debug")]
    spawn_context(|| name.clone(), inner);
    #[cfg(not(feature = "actor-debug"))]
    spawn(inner);
    f(self, tx).await
  }

  /// Creates a converter that converts a message of one type (even if not Signaled) into an actor's message.
  pub async fn adapter<F, W>(&mut self, mut wrap: W) -> ActorRef<F>
  where
    F: Send + Sync + 'static,
    W: FnMut(F) -> Vec<J> + Send + Sync + 'static,
  {
    let (tx, mut rx) = unbounded_channel::<F>();
    let mut me = self.tx.weak();
    let internal = async move {
      while let Some(j) = rx.recv().await {
        for j in (wrap)(j) {
          me.tell(j);
        }
      }
    };
    #[cfg(feature = "actor-debug")]
    {
      let name = format!("{}.adapter", self.name);
      spawn_context(|| name, internal);
    }
    #[cfg(not(feature = "actor-debug"))]
    spawn(internal);
    ActorRef::Strong {
      tx,
      #[cfg(feature = "actor-debug")]
      name: format!("{}.adapter", self.name),
    }
  }
}

/// Provides additional functions for spawned children.
pub struct SpawnContext<'j, J: Signaled, F: Signaled> {
  parent: &'j mut ActorContext<J>,
  name: String,
  child: ActorRef<F>,
}
impl<J: Signaled, F: Signaled> SpawnContext<'_, J, F> {
  /// Retrieves a child's reference.
  pub fn tx(self) -> ActorRef<F> {
    self.child
  }

  /// Creates a callback that will be invoked upon the child's death.
  /// There can be only one such callback for each child.
  pub fn watch<K: FnOnce() + Send + 'static>(self, j: K) -> Self {
    self.parent.children.get_mut(&self.name).unwrap().1.replace(Box::new(j));
    self
  }
}
impl<J: Signaled> Drop for ActorContext<J> {
  fn drop(&mut self) {
    #[cfg(feature = "actor-debug")]
    println!("{} !", self.name);
    let children = std::mem::take(&mut self.children);
    for (_, mut j) in children {
      j.0.tell(Signal::Stop);
      if let Some(j) = j.1.take() {
        j()
      }
    }
  }
}

/// The shortcuts for tell(Ok(_)) and tell(Err(_)).
pub trait OkErr<J> {
  fn ok(&mut self, j: J);
  fn err<E: Into<Error>>(&mut self, j: E);
}
impl<J: Send> OkErr<J> for ActorRef<Result<J>> {
  fn ok(&mut self, j: J) {
    self.tell(Ok(j));
  }

  fn err<E: Into<Error>>(&mut self, j: E) {
    self.tell(Err(j.into()));
  }
}

/// The shortcut for tell(Ok(())).
pub trait Ack {
  fn ack(&mut self);
}
impl Ack for ActorRef<Result<()>> {
  fn ack(&mut self) {
    self.ok(());
  }
}

/// Implements the Signaled trait for an enum containing a Signal variant.
/// This enum must be called Message and contain Signal(Signal) variant.
#[macro_export]
macro_rules! message {
  () => {
    impl From<Signal> for Message {
      fn from(j: Signal) -> Self {
        Self::Signal(j)
      }
    }

    impl TryFrom<Message> for Signal {
      type Error = Message;
      fn try_from(j: Message) -> std::result::Result<Self, Self::Error> {
        match j {
          Message::Signal(j) => Ok(j),
          #[allow(unused)]
          _ => Err(j),
        }
      }
    }
  };
}

mod imessage {
  use super::*;

  pub enum Message<T> {
    Value(T),
    Signal(Signal),
  }

  impl<T> From<Signal> for Message<T> {
    fn from(j: Signal) -> Self {
      Self::Signal(j)
    }
  }
  impl<T> TryFrom<Message<T>> for Signal {
    type Error = Message<T>;
    fn try_from(j: Message<T>) -> std::result::Result<Self, Self::Error> {
      match j {
        Message::Signal(j) => Ok(j),
        #[allow(unused)]
        _ => Err(j),
      }
    }
  }
}
