mod cache;

use env_logger::{Builder, Env};
use kaspa_rpc_core::{api::rpc::RpcApi, GetBlockDagInfoResponse};
use kaspa_wrpc_client::{KaspaRpcClient, WrpcEncoding};
use log::{info, LevelFilter};
use std::{thread, time};

#[tokio::main]
async fn main() {
    // Init Logger
    Builder::from_env(Env::default().default_filter_or("info"))
        .filter(None, LevelFilter::Info)
        .init();

    info!("Loading config...");
    let config = kaspalytics_utils::config::Config::from_env();

    info!("Creating RPC client...");
    let rpc_client = KaspaRpcClient::new(
        WrpcEncoding::Borsh,
        Some(&config.rpc_url),
        None,
        Some(config.network_id),
        None,
    )
    .unwrap();

    info!("Connecting RPC...");
    rpc_client.connect(None).await.unwrap();

    let mut cache = cache::Cache::default();

    let GetBlockDagInfoResponse {
        pruning_point_hash, ..
    } = rpc_client.get_block_dag_info().await.unwrap();

    let mut low_hash = pruning_point_hash;
    info!("Starting from low_hash {:?}", low_hash);

    loop {
        let GetBlockDagInfoResponse { tip_hashes, .. } =
            rpc_client.get_block_dag_info().await.unwrap();

        let blocks = rpc_client
            .get_blocks(Some(low_hash), true, true)
            .await
            .unwrap();

        for block in blocks.blocks.iter() {
            for tx in block.transactions.iter() {
                let tx_id = tx.verbose_data.as_ref().unwrap().transaction_id;

                cache
                    .transactions
                    .entry(tx_id)
                    .or_insert(cache::CacheTransaction::from(tx.clone()))
                    .blocks
                    .push(block.header.hash);
            }

            cache
                .blocks
                .insert(block.header.hash, cache::CacheBlock::from(block.clone()));

            if block.header.timestamp > cache.tip_timestamp {
                cache.tip_timestamp = block.header.timestamp;
            }
        }

        let vspc = rpc_client
            .get_virtual_chain_from_block(low_hash, true)
            .await
            .unwrap();
        for removed_chain_block in vspc.removed_chain_block_hashes.iter() {
            cache
                .blocks
                .entry(*removed_chain_block)
                .and_modify(|block| block.is_chain_block = false);

            let removed_transactions = cache
                .accepting_block_transactions
                .remove(removed_chain_block)
                .unwrap();

            for tx_id in removed_transactions.iter() {
                cache
                    .transactions
                    .entry(*tx_id)
                    .and_modify(|tx| tx.accepting_block_hash = None);
            }

            // info!("Removed former chain block {} (accepting txs {:?})", removed_chain_block, removed_transactions);
        }

        for acceptance in vspc.accepted_transaction_ids.iter() {
            if !cache.blocks.contains_key(&acceptance.accepting_block_hash) {
                break;
            }

            low_hash = acceptance.accepting_block_hash;

            cache
                .blocks
                .entry(acceptance.accepting_block_hash)
                .and_modify(|block| block.is_chain_block = true);

            for tx_id in acceptance.accepted_transaction_ids.iter() {
                cache.transactions.entry(*tx_id).and_modify(|tx| {
                    tx.accepting_block_hash = Some(acceptance.accepting_block_hash)
                });
            }

            cache.accepting_block_transactions.insert(
                acceptance.accepting_block_hash,
                acceptance.accepted_transaction_ids.clone(),
            );

            // info!(
            //     "Added chain block {} (accepting txs {:?})",
            //     acceptance.accepting_block_hash,
            //     acceptance.accepted_transaction_ids
            // );
        }

        cache.log_size();
        cache.prune();

        if tip_hashes.contains(&low_hash) {
            info!("at tip, sleeping");
            thread::sleep(time::Duration::from_millis(5000));
        }
    }
}
