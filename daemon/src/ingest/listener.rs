// This code is taken from example wRPC subscriber in rusty-kaspa repo
// (https://github.com/kaspanet/rusty-kaspa/blob/master/rpc/wrpc/examples/subscriber/src/main.rs)
// And adjusted as needed

pub use futures::{select_biased, FutureExt, StreamExt};
use kaspa_wrpc_client::prelude::*;
use kaspa_wrpc_client::result::Result;
use kaspalytics_utils::log::LogTarget;
use log::{error, info};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use workflow_core::channel::{Channel, DuplexChannel};
use workflow_core::task::spawn;

use crate::ingest::cache::Writer;
use crate::ingest::pipeline;

use super::cache::{DagCache, Reader};

struct Inner {
    // task control duplex channel - a pair of channels where sender
    // is used to signal an async task termination request and receiver
    // is used to signal task termination completion.
    task_ctl: DuplexChannel<()>,
    client: Arc<KaspaRpcClient>,
    is_connected: AtomicBool,
    // channel supplied to the notification subsystem
    // to receive the node notifications we subscribe to
    notification_channel: Channel<Notification>,
    // listener id used to manage notification scopes
    // we can have multiple IDs for different scopes
    // paired with multiple notification channels
    listener_id: Mutex<Option<ListenerId>>,
}

#[derive(Clone)]
pub struct Listener {
    inner: Arc<Inner>,
    dag_cache: Arc<DagCache>,
}

impl Listener {
    pub fn try_new(
        network_id: NetworkId,
        url: Option<String>,
        dag_cache: Arc<DagCache>,
    ) -> Result<Self> {
        let (resolver, url) = if let Some(url) = url {
            (None, Some(url))
        } else {
            (Some(Resolver::default()), None)
        };

        let client = Arc::new(KaspaRpcClient::new_with_args(
            WrpcEncoding::Borsh,
            url.as_deref(),
            resolver,
            Some(network_id),
            None,
        )?);

        let inner = Inner {
            task_ctl: DuplexChannel::oneshot(),
            client,
            is_connected: AtomicBool::new(false),
            notification_channel: Channel::unbounded(),
            listener_id: Mutex::new(None),
        };

        Ok(Self {
            inner: Arc::new(inner),
            dag_cache,
        })
    }

    pub fn is_connected(&self) -> bool {
        self.inner.is_connected.load(Ordering::SeqCst)
    }

    pub async fn start(&self) -> Result<()> {
        let options = ConnectOptions {
            block_async_connect: false,
            ..Default::default()
        };

        self.start_event_task().await?;
        self.client().connect(Some(options)).await?;

        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        self.client().disconnect().await?;
        self.stop_event_task().await?;

        Ok(())
    }

    pub fn client(&self) -> &Arc<KaspaRpcClient> {
        &self.inner.client
    }

    async fn register_notification_listeners(&self) -> Result<()> {
        // IMPORTANT: notification scopes are managed by the node
        // for the lifetime of the RPC connection, as such they
        // are "lost" if we disconnect. For that reason we must
        // re-register all notification scopes when we connect.

        let listener_id = self
            .client()
            .rpc_api()
            .register_new_listener(ChannelConnection::new(
                "dag-ingest",
                self.inner.notification_channel.sender.clone(),
                ChannelType::Persistent,
            ));

        *self.inner.listener_id.lock().unwrap() = Some(listener_id);

        self.client()
            .rpc_api()
            .start_notify(listener_id, Scope::BlockAdded(BlockAddedScope {}))
            .await?;

        Ok(())
    }

    pub async fn unregister_notification_listener(&self) -> Result<()> {
        let listener_id = self.inner.listener_id.lock().unwrap().take();
        if let Some(id) = listener_id {
            self.client().rpc_api().unregister_listener(id).await?;
        }
        Ok(())
    }

    async fn handle_notification(&self, notification: Notification) -> Result<()> {
        match notification {
            Notification::BlockAdded(bn) => {
                if self.dag_cache.tip_timestamp() < bn.block.header.timestamp {
                    self.dag_cache.set_tip_timestamp(bn.block.header.timestamp);
                }

                pipeline::block_add_pipeline(self.dag_cache.clone(), &bn.block);
            }
            // Notification::VirtualChainChanged(vccn) => {
            //     // Process removed chain blocks
            //     for removed_chain_block in vccn.removed_chain_block_hashes.iter() {
            //         pipeline::remove_chain_block_pipeline(
            //             self.dag_cache.clone(),
            //             removed_chain_block,
            //         );
            //     }

            //     // Process added chain blocks
            //     for acceptance in vccn.accepted_transaction_ids.iter() {
            //         if !self
            //             .dag_cache
            //             .contains_block_hash(&acceptance.accepting_block_hash)
            //         {
            //             break;
            //         }

            //         self.dag_cache
            //             .set_last_known_chain_block(acceptance.accepting_block_hash);

            //         pipeline::add_chain_block_acceptance_pipeline(
            //             self.dag_cache.clone(),
            //             acceptance.clone(),
            //         );
            //     }
            // }
            _ => unimplemented!(),
        }

        Ok(())
    }

    async fn handle_connect(&self) -> Result<()> {
        info!(target: LogTarget::Daemon.as_str(), "DagIngest Listener connected to {:?}, starting notification listeners...", self.client().url());

        self.register_notification_listeners().await?;

        self.inner.is_connected.store(true, Ordering::SeqCst);

        Ok(())
    }

    async fn handle_disconnect(&self) -> Result<()> {
        info!(target: LogTarget::Daemon.as_str(), "DagIngest Listener disconnected from {:?}, stopping notification listeners...", self.client().url());

        self.unregister_notification_listener().await?;

        self.inner.is_connected.store(false, Ordering::SeqCst);

        Ok(())
    }

    async fn start_event_task(&self) -> Result<()> {
        let listener = self.clone();
        let rpc_ctl_channel = self.client().rpc_ctl().multiplexer().channel();
        let task_ctl_receiver = self.inner.task_ctl.request.receiver.clone();
        let task_ctl_sender = self.inner.task_ctl.response.sender.clone();
        let notification_receiver = self.inner.notification_channel.receiver.clone();

        spawn(async move {
            let result = async {
                loop {
                    select_biased! {
                        msg = rpc_ctl_channel.receiver.recv().fuse() => {
                            match msg {
                                Ok(msg) => {
                                    // handle RPC channel connection and disconnection events
                                    match msg {
                                        RpcState::Connected => {
                                            if let Err(err) = listener.handle_connect().await {
                                                error!(target: LogTarget::Daemon.as_str(), "Error in connect handler: {err}");
                                                return Err(err);
                                            }
                                        },
                                        RpcState::Disconnected => {
                                            if let Err(err) = listener.handle_disconnect().await {
                                                error!(target: LogTarget::Daemon.as_str(), "Error in disconnect handler: {err}");
                                                return Err(err);
                                            }
                                        }
                                    }
                                }
                                Err(err) => {
                                    error!(target: LogTarget::Daemon.as_str(), "RPC CTL channel error: {err}");
                                    panic!("Unexpected: RPC CTL channel closed, halting...");
                                }
                            }
                        }
                        notification = notification_receiver.recv().fuse() => {
                            match notification {
                                Ok(notification) => {
                                    if let Err(err) = listener.handle_notification(notification).await {
                                        error!(target: LogTarget::Daemon.as_str(), "Error while handling notification: {err}");
                                        return Err(err);
                                    }
                                }
                                Err(err) => {
                                    panic!("RPC notification channel error: {err}");
                                }
                            }
                        },
                        _ = task_ctl_receiver.recv().fuse() => {
                            break;
                        },
                    }
                }

                info!(target: LogTarget::Daemon.as_str(), "Event task existing...");

                if listener.is_connected() {
                    listener.handle_disconnect().await.unwrap_or_else(|err| error!(target: LogTarget::Daemon.as_str(), "{err}"));
                }

                Ok(())
            }.await;

            task_ctl_sender.send(()).await.unwrap();

            result
        });
        Ok(())
    }

    async fn stop_event_task(&self) -> Result<()> {
        self.inner
            .task_ctl
            .signal(())
            .await
            .expect("stop_event_task() signal error");
        Ok(())
    }
}
