/*
 *
 * Copyright 2025 gRPC authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 */

use super::{Executor, SharedExec};
use crate::transport::channel::BoxFuture;
use pin_project::pin_project;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tower::ServiceExt;
use tower::make::MakeService;
use tower_service::Service;
use tracing::trace;

/// How requests wait for a connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    /// A request joins the connect attempt that runs when it is sent, and gets the result
    /// of that attempt. `poll_ready` does not wait for the connection. Used by channels to
    /// one endpoint.
    ///
    /// While a connection is opened, requests wait in their own futures, not in the
    /// buffer of the channel. The buffer capacity does not limit them.
    Join,
    /// `poll_ready` waits until the connection is ready. Used by load-balanced endpoints,
    /// so that the balancer sends requests only to connected endpoints.
    WaitReady,
}

/// Service that opens a connection on demand and opens a new one when it fails.
///
/// Each connect attempt runs as a separate task on the endpoint executor. The attempt
/// finishes even if no request waits for it. In [`Mode::Join`], only the requests that
/// joined an attempt get its error. A later request always starts a new attempt, so a
/// cancelled request cannot leave an old connect error behind for an unrelated request.
pub(crate) struct Reconnect<M, Target>
where
    M: Service<Target>,
    M::Error: Into<crate::BoxError>,
{
    mk_service: M,
    state: State<M::Response>,
    target: Target,
    executor: SharedExec,
    mode: Mode,
    has_been_connected: bool,
    is_lazy: bool,
}

enum State<S> {
    Idle,
    Connecting(Attempt<S>),
    Connected(S),
    /// Only in [`Mode::WaitReady`]: the error of a failed attempt for the next `call`.
    Failed(crate::BoxError),
}

impl<S> State<S> {
    fn name(&self) -> &'static str {
        match self {
            State::Idle => "Idle",
            State::Connecting(_) => "Connecting",
            State::Connected(_) => "Connected",
            State::Failed(_) => "Failed",
        }
    }
}

impl<M, Target> Reconnect<M, Target>
where
    M: Service<Target>,
    M::Error: Into<crate::BoxError>,
{
    pub(crate) fn new(
        mk_service: M,
        target: Target,
        executor: SharedExec,
        mode: Mode,
        is_lazy: bool,
    ) -> Self {
        Reconnect {
            mk_service,
            state: State::Idle,
            target,
            executor,
            mode,
            has_been_connected: false,
            is_lazy,
        }
    }

    /// Checks whether requests join the running attempt instead of waiting in `poll_ready`.
    ///
    /// An eager channel waits for its first connection in `poll_ready`, so that
    /// `Channel::connect` can return the first connect error.
    fn joins_attempts(&self) -> bool {
        self.mode == Mode::Join && (self.is_lazy || self.has_been_connected)
    }
}

impl<M, Target, S, Request> Service<Request> for Reconnect<M, Target>
where
    M: Service<Target, Response = S>,
    M::Future: Send + 'static,
    S: Service<Request> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    Request: Send + 'static,
    crate::BoxError: From<M::Error> + From<S::Error>,
    Target: Clone,
    <M as tower_service::Service<Target>>::Error: Into<crate::BoxError>,
{
    type Response = S::Response;
    type Error = crate::BoxError;
    type Future = ResponseFuture<S::Future, S::Response>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Set when this call starts an attempt. An attempt can fail before `poll_ready`
        // looks at it, for example when the executor drops the task at once. The request
        // then joins that attempt and gets its error, and `poll_ready` does not start
        // attempts in a loop.
        let mut started_attempt = false;
        loop {
            let joins_attempts = self.joins_attempts();
            match &mut self.state {
                State::Idle => {
                    trace!("poll_ready; idle");
                    match self.mk_service.poll_ready(cx) {
                        Poll::Ready(r) => r?,
                        Poll::Pending => {
                            trace!("poll_ready; MakeService not ready");
                            return Poll::Pending;
                        }
                    }

                    let fut = self.mk_service.make_service(self.target.clone());
                    let fut = async move { fut.await.map_err(Into::into) };
                    self.state = State::Connecting(Attempt::spawn(&self.executor, fut));
                    started_attempt = true;
                }
                State::Connecting(attempt) if joins_attempts => {
                    trace!("poll_ready; connecting");
                    match attempt.peek() {
                        // The request joins the running attempt in `call`.
                        None => return Poll::Ready(Ok(())),
                        Some(Ok(service)) => self.state = State::Connected(service),
                        // The request joins the attempt that this call started.
                        Some(Err(_)) if started_attempt => return Poll::Ready(Ok(())),
                        Some(Err(error)) => {
                            // The error belongs to the requests that joined the attempt.
                            // Start a new attempt for this request.
                            trace!("poll_ready; previous attempt failed: {}", error);
                            self.state = State::Idle;
                        }
                    }
                }
                State::Connecting(attempt) => {
                    trace!("poll_ready; connecting, waiting for the connection");
                    let result = match attempt.poll_take(cx) {
                        Poll::Ready(result) => result,
                        Poll::Pending => {
                            trace!("poll_ready; not ready");
                            return Poll::Pending;
                        }
                    };
                    match result {
                        Ok(service) => self.state = State::Connected(service),
                        Err(error) => {
                            trace!("poll_ready; error");
                            if !(self.has_been_connected || self.is_lazy) {
                                self.state = State::Idle;
                                return Poll::Ready(Err(error));
                            }
                            tracing::debug!("reconnect::poll_ready: {:?}", error);
                            self.state = State::Failed(error);
                            return Poll::Ready(Ok(()));
                        }
                    }
                }
                State::Connected(inner) => {
                    trace!("poll_ready; connected");

                    self.has_been_connected = true;

                    match inner.poll_ready(cx) {
                        Poll::Ready(Ok(())) => {
                            trace!("poll_ready; ready");
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Pending => {
                            trace!("poll_ready; not ready");
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(_)) => {
                            trace!("poll_ready; error");
                            self.state = State::Idle;
                        }
                    }
                }
                State::Failed(_) => return Poll::Ready(Ok(())),
            }
        }
    }

    fn call(&mut self, request: Request) -> Self::Future {
        tracing::trace!("Reconnect::call");
        let joins_attempts = self.joins_attempts();
        match &mut self.state {
            State::Connected(service) => ResponseFuture::new(service.call(request)),
            State::Connecting(attempt) if joins_attempts => {
                ResponseFuture::join(attempt.clone(), request)
            }
            State::Failed(_) => {
                let State::Failed(error) = std::mem::replace(&mut self.state, State::Idle) else {
                    unreachable!("state checked above");
                };
                tracing::debug!("error: {}", error);
                ResponseFuture::error(error)
            }
            State::Idle | State::Connecting(_) => {
                panic!("service not ready; poll_ready must be called first")
            }
        }
    }
}

impl<M, Target> fmt::Debug for Reconnect<M, Target>
where
    M: Service<Target> + fmt::Debug,
    <M as tower_service::Service<Target>>::Error: Into<crate::BoxError>,
    Target: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Reconnect")
            .field("mk_service", &self.mk_service)
            .field("state", &self.state.name())
            .field("target", &self.target)
            .field("mode", &self.mode)
            .finish()
    }
}

/// Handle to one connect attempt that runs as a separate task.
///
/// `Reconnect` and every request that joined the attempt hold a handle. When the last
/// handle is dropped, the task is cancelled.
struct Attempt<S> {
    shared: Arc<Shared<S>>,
}

impl<S> Clone for Attempt<S> {
    fn clone(&self) -> Self {
        Attempt {
            shared: self.shared.clone(),
        }
    }
}

struct Shared<S> {
    slot: Mutex<Slot<S>>,
    _cancel_on_drop: DropGuard,
}

enum Slot<S> {
    /// The attempt runs.
    Running(Waiters),
    /// The attempt finished.
    Done(Result<S, SharedConnectError>),
    /// The only consumer took the result. See [`Attempt::poll_take`].
    Taken,
}

/// Tasks that wait for a running attempt.
#[derive(Default)]
struct Waiters {
    /// Waker of the `Reconnect` that waits in `poll_ready`.
    reconnect: Option<Waker>,
    /// Wakers of joined requests, by waiter id. A request removes its entry when it is
    /// dropped, so cancelled requests do not accumulate here.
    joined: HashMap<u64, Waker>,
    next_id: u64,
}

impl Waiters {
    fn wake_all(self) {
        self.reconnect.into_iter().for_each(Waker::wake);
        self.joined.into_values().for_each(Waker::wake);
    }
}

/// Stores the waker of `cx` in `slot`, unless the stored waker wakes the same task.
fn set_waker(slot: &mut Option<Waker>, cx: &Context<'_>) {
    match slot {
        Some(waker) if waker.will_wake(cx.waker()) => {}
        _ => *slot = Some(cx.waker().clone()),
    }
}

impl<S> Shared<S> {
    /// Stores `result` if the attempt still runs, and wakes every waiter.
    fn publish(&self, result: Result<S, SharedConnectError>) {
        let mut slot = lock(&self.slot);
        if !matches!(*slot, Slot::Running(_)) {
            return;
        }
        let Slot::Running(waiters) = std::mem::replace(&mut *slot, Slot::Done(result)) else {
            unreachable!("slot checked above");
        };
        drop(slot);
        waiters.wake_all();
    }
}

/// Publishes an error if the connect task ends without a result, for example because
/// the connector panicked or the executor dropped the task, before or after its first
/// poll. Without this, requests that joined the attempt would wait forever.
struct PublishOnDrop<S> {
    shared: Weak<Shared<S>>,
}

impl<S> Drop for PublishOnDrop<S> {
    fn drop(&mut self) {
        // After a normal result or a cancellation, this does nothing: the slot is no
        // longer running, or no handle is left.
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        let error = crate::ConnectError("connect attempt ended without a result".into());
        shared.publish(Err(SharedConnectError::new(Box::new(error))));
    }
}

impl<S: Clone + Send + 'static> Attempt<S> {
    /// Spawns `fut` on `executor` and returns a handle to its result.
    fn spawn<F>(executor: &SharedExec, fut: F) -> Self
    where
        F: Future<Output = Result<S, crate::BoxError>> + Send + 'static,
    {
        let cancel = CancellationToken::new();
        let shared = Arc::new(Shared {
            slot: Mutex::new(Slot::Running(Waiters::default())),
            _cancel_on_drop: cancel.clone().drop_guard(),
        });

        let weak: Weak<Shared<S>> = Arc::downgrade(&shared);
        // The guard moves into the task, so it runs even if the executor drops the task
        // before its first poll.
        let publish_on_drop = PublishOnDrop {
            shared: weak.clone(),
        };
        let task = async move {
            let _publish_on_drop = publish_on_drop;
            let Some(result) = cancel.run_until_cancelled(fut).await else {
                trace!("connect attempt cancelled");
                return;
            };
            if let Some(shared) = weak.upgrade() {
                shared.publish(result.map_err(SharedConnectError::new));
            }
        };
        executor.execute(task);

        Attempt { shared }
    }

    /// Returns a copy of the result if the attempt finished.
    fn peek(&self) -> Option<Result<S, SharedConnectError>> {
        match &*lock(&self.shared.slot) {
            Slot::Running(_) => None,
            Slot::Done(result) => Some(result.clone()),
            Slot::Taken => unreachable!("a joined attempt is never taken"),
        }
    }

    /// Returns a copy of the result for a joined request, or registers its waker.
    ///
    /// `waiter` holds the id of the request in [`Waiters::joined`] while it waits.
    fn poll_joined(
        &self,
        waiter: &mut Option<u64>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<S, SharedConnectError>> {
        match &mut *lock(&self.shared.slot) {
            Slot::Running(waiters) => {
                let id = *waiter.get_or_insert_with(|| {
                    waiters.next_id += 1;
                    waiters.next_id
                });
                let mut waker = waiters.joined.remove(&id);
                set_waker(&mut waker, cx);
                waiters.joined.extend(waker.map(|waker| (id, waker)));
                Poll::Pending
            }
            Slot::Done(result) => {
                *waiter = None;
                Poll::Ready(result.clone())
            }
            Slot::Taken => unreachable!("a joined attempt is never taken"),
        }
    }

    /// Removes a joined request that stops waiting.
    fn leave(&self, waiter: u64) {
        if let Slot::Running(waiters) = &mut *lock(&self.shared.slot) {
            waiters.joined.remove(&waiter);
        }
    }
}

impl<S> Attempt<S> {
    /// Takes the result for the only consumer of the attempt, or registers the waker of
    /// `cx`. Used when `poll_ready` waits, so that the caller gets the original error.
    fn poll_take(&self, cx: &mut Context<'_>) -> Poll<Result<S, crate::BoxError>> {
        let mut slot = lock(&self.shared.slot);
        match &mut *slot {
            Slot::Running(waiters) => {
                set_waker(&mut waiters.reconnect, cx);
                Poll::Pending
            }
            Slot::Done(_) => {
                let Slot::Done(result) = std::mem::replace(&mut *slot, Slot::Taken) else {
                    unreachable!("slot checked above");
                };
                Poll::Ready(result.map_err(SharedConnectError::into_inner))
            }
            Slot::Taken => panic!("connect attempt polled after its result was taken"),
        }
    }
}

/// Future that waits for the result of an attempt for one joined request.
///
/// Dropping the future removes its waker from the attempt.
struct JoinAttempt<S: Clone + Send + 'static> {
    attempt: Attempt<S>,
    waiter: Option<u64>,
}

impl<S: Clone + Send + 'static> Future for JoinAttempt<S> {
    type Output = Result<S, SharedConnectError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.attempt.poll_joined(&mut this.waiter, cx)
    }
}

impl<S: Clone + Send + 'static> Drop for JoinAttempt<S> {
    fn drop(&mut self) {
        if let Some(waiter) = self.waiter.take() {
            self.attempt.leave(waiter);
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Connect error that every request that joined an attempt receives.
///
/// Shows the same text as the original error, and returns the original error as its
/// source. Code that searches the source chain, such as the conversion to `Status`,
/// finds the original error.
#[derive(Clone, Debug)]
struct SharedConnectError(Arc<crate::BoxError>);

impl SharedConnectError {
    fn new(error: crate::BoxError) -> Self {
        SharedConnectError(Arc::new(error))
    }

    /// Returns the original error if no other request holds it. Otherwise returns `self`.
    fn into_inner(self) -> crate::BoxError {
        match Arc::try_unwrap(self.0) {
            Ok(error) => error,
            Err(shared) => Box::new(SharedConnectError(shared)),
        }
    }
}

impl fmt::Display for SharedConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl Error for SharedConnectError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref().as_ref())
    }
}

/// Future that resolves to the response or failure to connect.
#[pin_project]
pub(crate) struct ResponseFuture<F, T> {
    #[pin]
    inner: Inner<F, T>,
}

#[pin_project(project = InnerProj)]
enum Inner<F, T> {
    Future(#[pin] F),
    Join(BoxFuture<'static, Result<T, crate::BoxError>>),
    Error(Option<crate::BoxError>),
}

impl<F, T> ResponseFuture<F, T> {
    pub(crate) fn new(inner: F) -> Self {
        ResponseFuture {
            inner: Inner::Future(inner),
        }
    }

    pub(crate) fn error(error: crate::BoxError) -> Self {
        ResponseFuture {
            inner: Inner::Error(Some(error)),
        }
    }

    /// Waits for `attempt`, then sends `request` on its connection.
    fn join<S, Request>(attempt: Attempt<S>, request: Request) -> Self
    where
        S: Service<Request, Response = T> + Clone + Send + 'static,
        S::Future: Send + 'static,
        crate::BoxError: From<S::Error>,
        Request: Send + 'static,
        T: Send + 'static,
    {
        ResponseFuture {
            inner: Inner::Join(Box::pin(async move {
                let join = JoinAttempt {
                    attempt,
                    waiter: None,
                };
                // The handle is released when `join` completes, so that the attempt can be
                // cancelled when nothing else uses it.
                let service = join
                    .await
                    .map_err(|error| Box::new(error) as crate::BoxError)?;
                service.oneshot(request).await.map_err(Into::into)
            })),
        }
    }
}

impl<F, T> fmt::Debug for ResponseFuture<F, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseFuture").finish_non_exhaustive()
    }
}

impl<F, T, E> Future for ResponseFuture<F, T>
where
    F: Future<Output = Result<T, E>>,
    E: Into<crate::BoxError>,
{
    type Output = Result<T, crate::BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.project();
        match me.inner.project() {
            InnerProj::Future(fut) => fut.poll(cx).map_err(Into::into),
            InnerProj::Join(fut) => fut.as_mut().poll(cx),
            InnerProj::Error(e) => {
                let e = e.take().expect("Polled after ready.");
                Poll::Ready(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::body::Body;
    use crate::transport::{Channel, Endpoint};
    use crate::{Code, Status};
    use bytes::Bytes;
    use http::Uri;
    use http_body_util::Empty;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use std::convert::Infallible;
    use std::io;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::DuplexStream;
    use tokio::time::{Duration, sleep};

    /// What one connect attempt does.
    #[derive(Clone, Copy, Debug)]
    enum Plan {
        /// Fails with `ConnectionRefused` after the delay.
        Refuse(Duration),
        /// Connects to an in-memory HTTP/2 server after the delay.
        Serve(Duration),
        /// Panics after the delay, as a connector with a bug.
        Panic(Duration),
    }

    /// Connector that follows one plan per attempt and counts what happened.
    ///
    /// An attempt past the end of the plan fails at once with the error
    /// `connect attempt <n>`, so a test can tell which attempt a request saw.
    #[derive(Clone)]
    struct ScriptedConnector {
        plans: Arc<Vec<Plan>>,
        calls: Arc<AtomicUsize>,
        finished: Arc<AtomicUsize>,
    }

    impl ScriptedConnector {
        fn new(plans: Vec<Plan>) -> Self {
            Self {
                plans: Arc::new(plans),
                calls: Arc::new(AtomicUsize::new(0)),
                finished: Arc::new(AtomicUsize::new(0)),
            }
        }

        /// Gets the number of connect attempts that started.
        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        /// Gets the number of connect attempts that ran to the end.
        fn finished(&self) -> usize {
            self.finished.load(Ordering::SeqCst)
        }
    }

    impl Service<Uri> for ScriptedConnector {
        type Response = TokioIo<DuplexStream>;
        type Error = io::Error;
        type Future = BoxFuture<'static, io::Result<Self::Response>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _uri: Uri) -> Self::Future {
            let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
            let plan = self.plans.get(attempt).copied();
            let finished = self.finished.clone();
            Box::pin(async move {
                let result = match plan {
                    Some(Plan::Refuse(delay)) => {
                        sleep(delay).await;
                        Err(io::Error::from(io::ErrorKind::ConnectionRefused))
                    }
                    Some(Plan::Serve(delay)) => {
                        sleep(delay).await;
                        Ok(serve_in_memory())
                    }
                    Some(Plan::Panic(delay)) => {
                        sleep(delay).await;
                        panic!("connector bug");
                    }
                    None => Err(io::Error::other(format!("connect attempt {attempt}"))),
                };
                finished.fetch_add(1, Ordering::SeqCst);
                result
            })
        }
    }

    /// Starts an HTTP/2 server on one end of an in-memory stream and returns the other end.
    fn serve_in_memory() -> TokioIo<DuplexStream> {
        let (client, server) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            let service =
                hyper::service::service_fn(|_: http::Request<hyper::body::Incoming>| async {
                    Ok::<_, Infallible>(http::Response::new(Empty::<Bytes>::new()))
                });
            let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(server), service)
                .await;
        });
        TokioIo::new(client)
    }

    fn endpoint() -> Endpoint {
        Endpoint::from_static("http://example.test")
    }

    async fn send(channel: &Channel) -> Result<http::Response<Body>, crate::transport::Error> {
        channel
            .clone()
            .oneshot(http::Request::new(Body::empty()))
            .await
    }

    /// Starts `count` requests and waits until the channel starts a connect attempt.
    async fn start_requests(
        channel: &Channel,
        connector: &ScriptedConnector,
        count: usize,
    ) -> Vec<tokio::task::JoinHandle<Result<http::Response<Body>, crate::transport::Error>>> {
        let requests = (0..count)
            .map(|_| {
                let channel = channel.clone();
                tokio::spawn(async move { send(&channel).await })
            })
            .collect();
        while connector.calls() == 0 {
            tokio::task::yield_now().await;
        }
        // Let every request reach the channel and join the attempt.
        for _ in 0..count * 4 {
            tokio::task::yield_now().await;
        }
        requests
    }

    /// Starts a request, waits until the channel starts a connect attempt for it, and
    /// cancels the request.
    async fn cancel_while_connecting(channel: &Channel, connector: &ScriptedConnector) {
        let request = start_requests(channel, connector, 1).await.remove(0);
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn discards_connect_error_that_no_request_waited_for() {
        let connector = ScriptedConnector::new(vec![Plan::Refuse(Duration::from_millis(100))]);
        let channel = Channel::new(connector.clone(), endpoint());

        cancel_while_connecting(&channel, &connector).await;
        sleep(Duration::from_secs(60)).await;

        let err = send(&channel).await.unwrap_err();
        assert!(format!("{err:?}").contains("connect attempt 1"), "{err:?}");
        assert_eq!(connector.calls(), 2, "the later request made a new attempt");
    }

    #[tokio::test(start_paused = true)]
    async fn returns_connect_error_to_waiting_request() {
        let connector = ScriptedConnector::new(vec![Plan::Refuse(Duration::from_millis(100))]);
        let channel = Channel::new(connector.clone(), endpoint());

        let err = send(&channel).await.unwrap_err();

        assert!(format!("{err:?}").contains("ConnectionRefused"), "{err:?}");
        assert_eq!(connector.calls(), 1, "a current error is not retried");
    }

    #[tokio::test(start_paused = true)]
    async fn returns_slow_connect_error_to_waiting_request() {
        let connector = ScriptedConnector::new(vec![Plan::Refuse(Duration::from_secs(30))]);
        let channel = Channel::new(connector.clone(), endpoint());

        let err = send(&channel).await.unwrap_err();

        assert!(format!("{err:?}").contains("ConnectionRefused"), "{err:?}");
        assert_eq!(
            connector.calls(),
            1,
            "the result does not depend on the attempt time"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn reuses_connection_from_abandoned_attempt() {
        let connector = ScriptedConnector::new(vec![Plan::Serve(Duration::from_millis(100))]);
        let channel = Channel::new(connector.clone(), endpoint());

        cancel_while_connecting(&channel, &connector).await;
        sleep(Duration::from_secs(60)).await;
        assert_eq!(
            connector.finished(),
            1,
            "the attempt finished without a request"
        );

        let response = send(&channel).await.expect("request succeeds");
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(
            connector.calls(),
            1,
            "the request used the finished connection"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn requests_that_join_one_attempt_share_its_error() {
        let connector = ScriptedConnector::new(vec![Plan::Refuse(Duration::from_millis(100))]);
        let channel = Channel::new(connector.clone(), endpoint());

        for request in start_requests(&channel, &connector, 3).await {
            let err = request.await.unwrap().unwrap_err();
            assert!(format!("{err:?}").contains("ConnectionRefused"), "{err:?}");
        }
        assert_eq!(connector.calls(), 1, "all requests joined one attempt");
    }

    #[tokio::test(start_paused = true)]
    async fn requests_that_join_one_attempt_share_its_connection() {
        let connector = ScriptedConnector::new(vec![Plan::Serve(Duration::from_millis(100))]);
        let channel = Channel::new(connector.clone(), endpoint());

        for request in start_requests(&channel, &connector, 3).await {
            let response = request.await.unwrap().expect("request succeeds");
            assert_eq!(response.status(), http::StatusCode::OK);
        }
        assert_eq!(connector.calls(), 1, "all requests joined one attempt");
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_request_does_not_affect_other_joined_requests() {
        let connector = ScriptedConnector::new(vec![Plan::Serve(Duration::from_millis(100))]);
        let channel = Channel::new(connector.clone(), endpoint());

        let mut requests = start_requests(&channel, &connector, 2).await;
        let kept = requests.pop().unwrap();
        let cancelled = requests.pop().unwrap();
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());

        let response = kept.await.unwrap().expect("request succeeds");
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(connector.calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn request_timeout_includes_connect_time() {
        let connector = ScriptedConnector::new(vec![Plan::Serve(Duration::from_secs(5))]);
        let endpoint = endpoint().timeout(Duration::from_secs(1));
        let channel = Channel::new(connector.clone(), endpoint);

        let err = send(&channel).await.unwrap_err();
        assert!(format!("{err:?}").contains("TimeoutExpired"), "{err:?}");

        // The attempt continues after the request times out, and a later request uses it.
        sleep(Duration::from_secs(10)).await;
        let response = send(&channel).await.expect("request succeeds");
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(connector.calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn connector_panic_fails_joined_requests_and_next_request_reconnects() {
        let connector = ScriptedConnector::new(vec![
            Plan::Panic(Duration::from_millis(100)),
            Plan::Serve(Duration::from_millis(100)),
        ]);
        let channel = Channel::new(connector.clone(), endpoint());

        for request in start_requests(&channel, &connector, 2).await {
            let err = request.await.unwrap().unwrap_err();
            assert!(format!("{err:?}").contains("without a result"), "{err:?}");
        }

        let response = send(&channel).await.expect("the next request reconnects");
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(connector.calls(), 2);
    }

    #[tokio::test]
    async fn joined_connect_error_maps_to_unavailable() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect_lazy();

        let err = send(&channel).await.unwrap_err();
        let status = Status::from_error(Box::new(err));

        assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    }

    type TestService = tower::util::BoxCloneService<(), (), crate::BoxError>;
    type TestReconnect = Reconnect<tower::util::BoxService<(), TestService, crate::BoxError>, ()>;

    /// Makes a `Reconnect` whose connect attempts follow `plans`, like `ScriptedConnector`,
    /// but produce a service that always succeeds.
    fn reconnect_with(
        plans: Vec<Plan>,
        mode: Mode,
        is_lazy: bool,
    ) -> (TestReconnect, ScriptedConnector) {
        let connector = ScriptedConnector::new(plans);
        let mut inner = connector.clone();
        let mk_service = tower::service_fn(move |()| {
            let connect = inner.call(Uri::from_static("http://example.test"));
            async move {
                connect.await?;
                let service = tower::service_fn(|()| async { Ok::<_, crate::BoxError>(()) });
                Ok::<_, crate::BoxError>(TestService::new(service))
            }
        });
        let reconnect = Reconnect::new(
            tower::util::BoxService::new(mk_service),
            (),
            SharedExec::tokio(),
            mode,
            is_lazy,
        );
        (reconnect, connector)
    }

    fn poll_ready_now(reconnect: &mut TestReconnect) -> Poll<Result<(), crate::BoxError>> {
        Service::<()>::poll_ready(reconnect, &mut Context::from_waker(Waker::noop()))
    }

    #[tokio::test(start_paused = true)]
    async fn attempt_is_cancelled_when_last_handle_drops() {
        let (mut reconnect, connector) = reconnect_with(
            vec![Plan::Refuse(Duration::from_secs(60))],
            Mode::Join,
            true,
        );

        assert!(matches!(
            poll_ready_now(&mut reconnect),
            Poll::Ready(Ok(()))
        ));
        let joined = Service::<()>::call(&mut reconnect, ());
        tokio::task::yield_now().await;
        drop(reconnect);
        drop(joined);
        sleep(Duration::from_secs(120)).await;

        assert_eq!(connector.calls(), 1);
        assert_eq!(connector.finished(), 0, "the attempt was cancelled");
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_joined_requests_leave_no_wakers() {
        let (mut reconnect, _connector) = reconnect_with(
            vec![Plan::Refuse(Duration::from_secs(120))],
            Mode::Join,
            true,
        );
        assert!(matches!(
            poll_ready_now(&mut reconnect),
            Poll::Ready(Ok(()))
        ));

        let mut joined: Vec<_> = (0..100)
            .map(|_| Service::<()>::call(&mut reconnect, ()))
            .collect();
        for request in &mut joined {
            let polled = Pin::new(request).poll(&mut Context::from_waker(Waker::noop()));
            assert!(polled.is_pending());
        }
        assert_eq!(joined_waiters(&reconnect), 100);

        drop(joined);
        assert_eq!(
            joined_waiters(&reconnect),
            0,
            "cancelled requests removed their wakers"
        );
    }

    /// Gets the number of joined requests that wait for the running attempt.
    fn joined_waiters(reconnect: &TestReconnect) -> usize {
        let State::Connecting(attempt) = &reconnect.state else {
            panic!("expected a running attempt, got {}", reconnect.state.name());
        };
        match &*lock(&attempt.shared.slot) {
            Slot::Running(waiters) => waiters.joined.len(),
            Slot::Done(_) | Slot::Taken => panic!("expected a running attempt"),
        }
    }

    /// Executor that drops every task without running it, as a runtime that shuts down.
    struct DropExec;

    impl Executor<BoxFuture<'static, ()>> for DropExec {
        fn execute(&self, fut: BoxFuture<'static, ()>) {
            drop(fut);
        }
    }

    #[tokio::test]
    async fn executor_that_drops_the_task_fails_the_request_without_looping() {
        let (reconnect, connector) =
            reconnect_with(vec![Plan::Serve(Duration::ZERO)], Mode::Join, true);
        let mut reconnect = Reconnect {
            executor: SharedExec::new(DropExec),
            ..reconnect
        };

        for attempt in 1..=2 {
            assert!(matches!(
                poll_ready_now(&mut reconnect),
                Poll::Ready(Ok(()))
            ));
            let err = Service::<()>::call(&mut reconnect, ()).await.unwrap_err();
            assert!(format!("{err:?}").contains("without a result"), "{err:?}");
            assert_eq!(connector.calls(), attempt, "one attempt per request");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn attempt_runs_while_a_joined_request_waits() {
        let (mut reconnect, connector) = reconnect_with(
            vec![Plan::Refuse(Duration::from_secs(60))],
            Mode::Join,
            true,
        );

        assert!(matches!(
            poll_ready_now(&mut reconnect),
            Poll::Ready(Ok(()))
        ));
        let joined = Service::<()>::call(&mut reconnect, ());
        drop(reconnect);
        let err = joined.await.unwrap_err();

        assert!(format!("{err:?}").contains("ConnectionRefused"), "{err:?}");
        assert_eq!(
            connector.finished(),
            1,
            "the joined request kept the attempt alive"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn eager_connect_returns_first_error_from_poll_ready() {
        let (mut reconnect, _connector) = reconnect_with(
            vec![Plan::Refuse(Duration::from_millis(100))],
            Mode::Join,
            false,
        );

        assert!(poll_ready_now(&mut reconnect).is_pending());
        sleep(Duration::from_secs(1)).await;
        let Poll::Ready(Err(err)) = poll_ready_now(&mut reconnect) else {
            panic!("the first connect error comes from poll_ready");
        };

        assert!(err.downcast_ref::<io::Error>().is_some(), "{err:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn wait_ready_mode_waits_and_returns_original_error() {
        let (mut reconnect, _connector) = reconnect_with(
            vec![Plan::Refuse(Duration::from_millis(100))],
            Mode::WaitReady,
            true,
        );

        assert!(
            poll_ready_now(&mut reconnect).is_pending(),
            "waits for the connection"
        );
        sleep(Duration::from_secs(1)).await;
        assert!(matches!(
            poll_ready_now(&mut reconnect),
            Poll::Ready(Ok(()))
        ));
        let err = Service::<()>::call(&mut reconnect, ()).await.unwrap_err();

        assert!(err.downcast_ref::<io::Error>().is_some(), "{err:?}");
    }
}
