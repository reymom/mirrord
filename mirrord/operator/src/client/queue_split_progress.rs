//! Reports what a starting session's queue split is waiting on.

use std::{future::Future, ops::Not, time::Duration};

use kube::{Api, Client, api::ListParams};
use mirrord_progress::Progress;
use tokio::time::Instant;

use crate::crd::queue_split::QueueSplit;

const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// How long a split may wait before mirrord warns about it.
const WARN_AFTER: Duration = Duration::from_secs(30);

/// Shows what the queue split owned by one session waits on as a progress task.
///
/// Reads the operator's queue split view, so an operator without it, or a user not allowed to
/// read it, gets no report. Dropping it without [`QueueSplitProgress::finish`] fails the open
/// task.
pub(crate) struct QueueSplitProgress<'a, P: Progress> {
    api: Api<QueueSplit>,
    session: String,
    progress: &'a P,
    task: Option<P>,
    message: Option<String>,
    waiting_since: Option<Instant>,
    warned: bool,
    available: bool,
}

impl<'a, P: Progress> QueueSplitProgress<'a, P> {
    pub(crate) fn new(client: Client, namespace: &str, session: String, progress: &'a P) -> Self {
        Self {
            api: Api::namespaced(client, namespace),
            session,
            progress,
            task: None,
            message: None,
            waiting_since: None,
            warned: false,
            available: true,
        }
    }

    /// Runs `future` while reporting the split.
    pub(crate) async fn run<T, E>(
        mut self,
        future: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        let output = {
            let report = async {
                loop {
                    tokio::time::sleep(POLL_INTERVAL).await;
                    self.poll().await;
                }
            };

            tokio::select! {
                output = future => output,
                _ = report => unreachable!("reporting never ends"),
            }
        };

        if output.is_ok() {
            self.finish();
        }

        output
    }

    /// Reads the split once and reports any change in what it waits on.
    pub(crate) async fn poll(&mut self) {
        if !self.available {
            return;
        }

        let splits = match self.api.list(&ListParams::default()).await {
            Ok(splits) => splits.items,
            Err(kube::Error::Api(status)) if matches!(status.code, 403 | 404) => {
                tracing::debug!(
                    ?status,
                    "Queue split view unavailable, not reporting the split"
                );
                self.available = false;

                return;
            }
            Err(error) => {
                tracing::debug!(%error, "Failed to read the queue split view");

                return;
            }
        };

        let Some(message) = waiting_message(
            splits
                .iter()
                .filter(|split| split.spec.session.eq_ignore_ascii_case(&self.session)),
        ) else {
            return;
        };

        if self.message.as_ref() != Some(&message) {
            if let Some(mut task) = self.task.take() {
                task.success(None);
            }

            self.task = Some(self.progress.subtask(&message));
        }

        let waiting_since = *self.waiting_since.get_or_insert_with(Instant::now);
        if !self.warned && waiting_since.elapsed() >= WARN_AFTER {
            self.progress
                .warning(&format!("queue splitting is {message}"));
            self.warned = true;
        }

        self.message = Some(message);
    }

    /// Completes the open task, if any.
    pub(crate) fn finish(mut self) {
        if let Some(mut task) = self.task.take() {
            task.success(None);
        }
    }
}

/// Describes the target pods `splits` wait on, while they wait on any.
///
/// A multi-cluster session has one split per workload cluster.
fn waiting_message<'a>(splits: impl IntoIterator<Item = &'a QueueSplit>) -> Option<String> {
    let pods = splits
        .into_iter()
        .filter_map(|split| split.status.as_ref())
        .filter(|status| status.phase == "Pending")
        .flat_map(|status| &status.target_pods)
        .filter(|pod| pod.ready.not())
        .map(|pod| {
            let reason = pod.reason.as_deref().unwrap_or("not ready");

            format!("pod `{}`: {reason}", pod.name)
        })
        .collect::<Vec<_>>();

    pods.is_empty().not().then(|| {
        format!(
            "waiting for a target pod to restart with the split queues and become ready: {}",
            pods.join("; ")
        )
    })
}

#[cfg(test)]
mod tests {
    use super::waiting_message;
    use crate::crd::{
        queue_split::{QueueSplit, QueueSplitSpec, QueueSplitStatus, QueueSplitTargetPod},
        session::{KubeResourceTarget, SessionOwner, SessionTarget},
    };

    fn split(phase: &str, target_pods: Vec<QueueSplitTargetPod>) -> QueueSplit {
        let mut split = QueueSplit::new(
            "a1b2.consumer.deployment",
            QueueSplitSpec {
                session: "A1B2".to_owned(),
                target: SessionTarget::KubeResource(KubeResourceTarget {
                    api_version: "apps/v1".to_owned(),
                    kind: "Deployment".to_owned(),
                    name: "consumer".to_owned(),
                    container: "app".to_owned(),
                }),
                owner: SessionOwner {
                    user_id: "user".to_owned(),
                    username: "user".to_owned(),
                    hostname: "host".to_owned(),
                    k8s_username: "user".to_owned(),
                },
                filters: Vec::new(),
            },
        );
        split.status = Some(QueueSplitStatus {
            phase: phase.to_owned(),
            target_pods,
            ..Default::default()
        });
        split
    }

    fn pod(name: &str, ready: bool, reason: Option<&str>) -> QueueSplitTargetPod {
        QueueSplitTargetPod {
            name: name.to_owned(),
            patched: true,
            ready,
            reason: reason.map(ToOwned::to_owned),
        }
    }

    #[test]
    fn pending_split_lists_the_pods_that_are_not_ready() {
        let split = split(
            "Pending",
            vec![
                pod("a", false, Some("container `app` is running but not ready")),
                pod("b", true, None),
                pod("c", false, None),
            ],
        );

        assert_eq!(
            waiting_message([&split]).as_deref(),
            Some(
                "waiting for a target pod to restart with the split queues and become ready: \
                 pod `a`: container `app` is running but not ready; pod `c`: not ready"
            )
        );
    }

    #[test]
    fn splits_on_several_clusters_list_every_pending_pod() {
        let east = split("Pending", vec![pod("a", false, Some("starting"))]);
        let west = split("Pending", vec![pod("b", false, Some("unschedulable"))]);
        let done = split("Ready", vec![pod("c", true, None)]);

        assert_eq!(
            waiting_message([&east, &west, &done]).as_deref(),
            Some(
                "waiting for a target pod to restart with the split queues and become ready: \
                 pod `a`: starting; pod `b`: unschedulable"
            )
        );
    }

    #[test]
    fn split_without_pending_pods_waits_on_nothing() {
        assert_eq!(waiting_message([&split("Pending", Vec::new())]), None);
        assert_eq!(
            waiting_message([&split("Ready", vec![pod("a", false, Some("starting"))])]),
            None
        );
        assert_eq!(
            waiting_message([&split("Failed", vec![pod("a", false, Some("starting"))])]),
            None
        );
    }
}
