use std::collections::HashMap;
use log::info;
use solana_client::rpc_client::RpcClient;
use solana_sdk::clock::Epoch;
use solana_sdk::pubkey::Pubkey;
use solana_foundation_delegation_program_registry::state::Participant;

use crate::{Cluster, Config, EpochClassification, get_reported_performance_metrics};

pub fn backfill_self_reporting(config: &Config, rpc_client: &RpcClient,
                               all_participants: &HashMap<Pubkey, Participant>,
) -> Result<(), Box<dyn std::error::Error>> {
    backfill_cluster(&config.cluster, rpc_client, &config, all_participants)?;
    Ok(())
}

fn backfill_cluster(cluster: &Cluster, rpc_client: &RpcClient, config: &Config,
                    all_participants: &HashMap<Pubkey, Participant>,
) -> Result<Epoch, Box<dyn std::error::Error>> {
    info!("backfill_cluster {:?}", cluster);
    let mut epoch_to_try = rpc_client.get_epoch_info()?.epoch;

    // go through previous epochs until there is no self-reporting data
    loop {
        match backfill_epoch(config, &rpc_client, cluster, epoch_to_try,
                             all_participants,
        ) {
            Ok(last_loaded_epoch) => {
                info!("last loaded epoch: {:?}", last_loaded_epoch);
                epoch_to_try = last_loaded_epoch;
            }
            Err(e) => {
                info!("backfill_epoch not okay for {:?}", epoch_to_try);
                info!("{:?}", e);
                return Err("bah".into())
            }
        }
    }
}

/// returns epoch that was loaded
pub fn backfill_epoch(
    config: &Config,
    rpc_client: &RpcClient,
    cluster: &Cluster,
    epoch: Epoch,
    all_participants: &HashMap<Pubkey, Participant>,
) -> Result<Epoch, Box<dyn std::error::Error>> {
    info!("backfill_epoch {:?}/{:?}", cluster, epoch);
    let performance_db_url = config.performance_db_url.as_ref().unwrap();
    let performance_db_token = config.performance_db_token.as_ref().unwrap();

    let (mut subsequent_epoch, epoch_classification) = EpochClassification::load_previous(epoch + 1, &config.cluster_db_path_for(*cluster))?.unwrap();

    info!("subsequent_epoch: {:?}", subsequent_epoch);

    let mut epoch_classification = epoch_classification.into_current().clone();

    // let participants_for_epoch = epoch_classification
    //     .validator_classifications.as_ref().unwrap().iter()
    //     .map(|(pubkey, classification)| {
    //         classification.identity
    //     }).collect();

    let reported_metrics = get_reported_performance_metrics(
        &performance_db_url,
        &performance_db_token,
        &config.cluster,
        &rpc_client,
        &(subsequent_epoch - 1),
        &all_participants
    )?;

    // now update
    epoch_classification.validator_classifications
        // .as_ref()
        .as_mut()
        .unwrap()
        .iter_mut()
        .for_each(|(_pk, classification)| {
            match reported_metrics.get(&classification.identity) {
                Some(rm) => classification.self_reported_metrics = Some(rm.clone()),
                None => info!("hey")
            }
        });

    // now persist epoch_classification
    EpochClassification::new(epoch_classification).save(subsequent_epoch, &config.cluster_db_path())?;

    Ok(subsequent_epoch - 1)
}
