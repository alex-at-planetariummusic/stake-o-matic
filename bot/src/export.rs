use {
    crate::data_center_info::{DataCenterId, DataCenterInfo},
    crate::rpc_client_utils::{get_vote_account_info, VoteAccountInfo},
    crate::{
        classify_producers, db::*, get_confirmed_blocks, get_self_stake_by_vote_account, BoxResult,
        Config,
    },
    bincode::deserialize,
    lazy_static::lazy_static,
    log::*,
    native_tls::TlsConnector,
    postgres::types::Json,
    postgres::{Client, Transaction},
    postgres_native_tls::MakeTlsConnector,
    regex::Regex,
    serde::{Deserialize, Serialize},
    serde_json::{Map, Number, Value},
    solana_client::rpc_client::RpcClient,
    solana_config_program::{get_config_data, ConfigKeys},
    solana_foundation_delegation_program_cli::get_participants_with_state,
    solana_sdk::pubkey::Pubkey,
    solana_sdk::{clock::Epoch, commitment_config::CommitmentConfig},
    std::collections::HashMap,
    std::convert::TryFrom,
    thiserror::private::DisplayAsDisplay,
};

/// Exports data for the epoch specified in config.epoch to a postgres database
pub fn export_to_db(epoch: Epoch, config: &Config, rpc_client: &RpcClient) -> BoxResult<()> {
    info!("Exporting data to database for epoch {}", epoch);

    let db_path = &config.cluster_db_path_for(config.cluster);
    info!(
        "DB path is: {}",
        std::fs::canonicalize(&db_path).unwrap().display()
    );

    // if Some, the DB transactions will occur
    let db_transaction_client: Option<Client>;

    if config.dry_run {
        info!("Dry run: not updating database. Use --confirm to actually update the database");
        db_transaction_client = None;
    } else {
        let db_url = std::env::var("DB_URL");
        if db_url.is_err() {
            return Err(
                "environment variable DB_URL must be set to export data to a database".into(),
            );
        }
        let db_params = &*db_url.unwrap();
        let connector = TlsConnector::builder()
            .danger_accept_invalid_hostnames(true)
            .danger_accept_invalid_certs(true)
            .build()?;

        let connector = MakeTlsConnector::new(connector);

        db_transaction_client = Some(Client::connect(db_params, connector)?);
    }

    // yml files provide data that explains why validators received stake in a given epoch. So, e.g. `epoch-184.yml` provides information about what happened in epoch 183.
    let epoch_classification = EpochClassification::load(epoch + 1, db_path);
    if epoch_classification.is_err() {
        return Err("Could not load epoch information from yml".into());
    }

    let epoch_schedule = rpc_client.get_epoch_schedule()?;
    let first_slot_in_epoch = epoch_schedule.get_first_slot_in_epoch(epoch);
    let last_slot_in_epoch = epoch_schedule.get_last_slot_in_epoch(epoch);

    let epoch_classification = epoch_classification.unwrap().into_current();

    let epoch_notes = read_epoch_notes(&epoch_classification.notes);

    let validator_classifications = epoch_classification.validator_classifications.unwrap();

    let data_center_info = epoch_classification.data_center_info;
    let id_to_data_center_info: HashMap<DataCenterId, DataCenterInfo> = data_center_info
        .into_iter()
        .map(|dci| (dci.id.clone(), dci))
        .collect();

    let (vote_account_info, _total_active_stake) = get_vote_account_info(&rpc_client, epoch)?;
    let key_to_info: HashMap<Pubkey, VoteAccountInfo> = vote_account_info
        .into_iter()
        .map(|vai| (vai.identity, vai))
        .collect();

    // TODO: save skip rate in yaml, and only make these rpc calls if skip rate is not found in yaml
    // This gives us Err if the first/last slots are outside the range that rpc_client.get_account_with_commitment returns
    // let confirmed_blocks =
    //     get_confirmed_blocks(rpc_client, first_slot_in_epoch, last_slot_in_epoch);

    // let blocks_and_slots;

    // if confirmed_blocks.is_err() {
    //     // info!("could not get confirmed blocks; skip rate cannot be recorded");
    //     // blocks_and_slots = None;
    //     error!("{:?}", confirmed_blocks.err());
    //     return Err("Could not get confirmed blocks; skip rate cannot be recorded".into());
    // } else {
    //     let leader_schedule = rpc_client
    //         .get_leader_schedule_with_commitment(
    //             Some(first_slot_in_epoch),
    //             CommitmentConfig::finalized(),
    //         )?
    //         .unwrap();
    //
    //     let (
    //         _quality_block_producers,
    //         _poor_block_producers,
    //         _block_producer_classification_reason,
    //         _cluster_average_skip_rate,
    //         _too_many_poor_block_producers,
    //         bns,
    //     ) = classify_producers(
    //         first_slot_in_epoch,
    //         confirmed_blocks.unwrap(),
    //         leader_schedule,
    //         &config,
    //     )?;
    //     blocks_and_slots = Some(bns);
    // }

    let (vote_account_info, _total_active_stake) = get_vote_account_info(&rpc_client, epoch)?;

    // This takes a long time. A candidate for data that should be put in the yaml files
    let self_stake_by_vote_account =
        get_self_stake_by_vote_account(rpc_client, epoch, &vote_account_info)?;

    let mut validator_stats: HashMap<Pubkey, Map<String, Value>> = HashMap::new();

    for (validator_pk, _) in validator_classifications.clone() {
        let validator_classification = validator_classifications.get(&validator_pk);
        if validator_classification.is_none() {
            info!("No validator classification for {}; skipping", validator_pk);
            continue;
        }
        let validator_classification = validator_classification.unwrap();
        let current_data_center = validator_classification
            .current_data_center
            .as_ref()
            .unwrap();
        let data_center_info = id_to_data_center_info.get(current_data_center).unwrap();
        if key_to_info.get(&validator_pk).is_none() {
            info!("No vote account info for {}; skipping", validator_pk);
            continue;
        }
        let vote_account_info = key_to_info.get(&validator_pk).unwrap();

        let vote_address = vote_account_info.vote_address;
        let self_stake = self_stake_by_vote_account.get(&vote_address).unwrap_or(&0);

        let mut stats = Map::new();

        stats.insert(
            "state".to_string(),
            serde_json::to_value(&validator_classification.stake_state).unwrap(),
        );
        stats.insert(
            "state_reason".to_string(),
            serde_json::to_value(&validator_classification.stake_state_reason).unwrap(),
        );
        stats.insert(
            "state_action".to_string(),
            serde_json::to_value(&validator_classification.stake_action).unwrap(),
        );
        stats.insert(
            "notes".to_string(),
            serde_json::to_value(&validator_classification.notes)?,
        );

        if validator_classification.blocks.is_some() && validator_classification.slots.is_some() {
            let blocks = validator_classification.blocks.unwrap();
            let slots = validator_classification.slots.unwrap();
            info!("b/s {}/{}", blocks, slots);
            stats.insert("blocks".to_string(), serde_json::to_value(blocks)?);
            stats.insert("slots".to_string(), serde_json::to_value(slots)?);
            info!("stats: {:?}", stats);
        } else {
            info!("No B/S for {:?}", validator_classification.participant);
            // return Err("Could not find blocks/slots".into());
        }

        // if blocks_and_slots.is_some() {
        //     let b_n_s = blocks_and_slots.clone().unwrap();
        //     let val_b_n_s = b_n_s.get(&validator_pk);
        //     if let Some((blocks, slots)) = val_b_n_s {
        //         stats.insert("blocks".to_string(), serde_json::to_value(blocks)?);
        //         stats.insert("slots".to_string(), serde_json::to_value(slots)?);
        //     }
        // }

        stats.insert(
            "vote_credits".to_string(),
            vote_account_info.epoch_credits.into(),
        );
        stats.insert(
            "commission".to_string(),
            vote_account_info.commission.into(),
        );
        stats.insert(
            "epoch_data_center".to_string(),
            serde_json::to_value(current_data_center.as_display())?,
        );
        stats.insert(
            "data_center_stake".to_string(),
            data_center_info.stake.into(),
        );
        stats.insert(
            "data_center_stake_percent".to_string(),
            data_center_info.stake_percent.into(),
        );
        stats.insert("self_stake".to_string(), serde_json::to_value(self_stake)?);

        validator_stats.insert(validator_pk, stats);
    }

    if let Some(mut db_tx_client) = db_transaction_client {
        let mut db_transaction = db_tx_client.transaction()?;

        let validator_names = get_validator_names(&rpc_client)?;

        persist_validator_stats(
            validator_stats,
            &mut db_transaction,
            config,
            epoch,
            validator_names,
        )?;

        info!("persisting notes");
        persist_epoch_notes(&epoch_notes, &mut db_transaction, config, epoch)?;

        info!("updating participant keypairs");
        update_keypair_table(&mut db_transaction)?;

        info!("committing the transaction");
        let res = db_transaction.commit();
        if res.is_err() {
            info!("DB transaction error: {:?}", res.err());
            return Err("DB transaction failed".into());
        }
    }

    Ok(())
}

/// Updates the ValidatorKeyPair table
fn update_keypair_table(transaction: &mut Transaction<'_>) -> BoxResult<()> {
    let participants = get_participants_with_state(
        &RpcClient::new("https://api.mainnet-beta.solana.com".to_string()),
        None,
    )?;

    info!("Got {} participants", participants.len());

    for (pubkey, participant) in participants {
        // info!("SELECTing {:?}", &pubkey);
        let rows = transaction.query(
            r#"SELECT id, mainnet_beta_pk, testnet_pk, state
            FROM "ValidatorKeyPair"
            WHERE pubkey=$1"#,
            &[
                &pubkey.to_string()
            ],
        )?;
        info!("got {:?}", &pubkey);

        // this strange incantation get the string without surrounding quotation marks
        let previous_state = &serde_json::to_value(&participant.state)?
            .as_str()
            .unwrap()
            .to_string();


        if rows.is_empty() {
            // info!("INSERTing {:?}", &pubkey);
            transaction.execute(
                r#"INSERT INTO "ValidatorKeyPair"
                                    (mainnet_beta_pk, testnet_pk, state, pubkey)
                                    VALUES ($1, $2, $3, $4)"#,
                &[
                    &participant.mainnet_identity.to_string(),
                    &participant.testnet_identity.to_string(),
                    previous_state,
                    &pubkey.to_string()
                ],
            )?;
        } else if rows.len() == 1 {
            info!("UPDATEing {:?}", &pubkey);
            let row = rows.first().unwrap();

            let current_state: String = row.get("state");
            let current_mk: String = row.get("mainnet_beta_pk");
            let current_tk: String = row.get("testnet_pk");

            if &current_state != previous_state ||
                &current_mk != &participant.mainnet_identity.to_string() ||
                &current_tk != &participant.testnet_identity.to_string() {
                let id: i32 = row.get("id");

                info!("Updating {}", id);

                transaction.execute(
                    r#"UPDATE "ValidatorKeyPair"
                SET state=$1,
                    mainnet_beta_pk=$2,
                    testnet_pk=$3
                WHERE id=$2"#,
                    &[
                        &previous_state,
                        &participant.mainnet_identity.to_string(),
                        &participant.testnet_identity.to_string(),
                        &id
                    ],
                )?;
            }
        } else {
            // DB constraints _should_ make it so this never happens
            return Err(format!(
                "More than one row in ValidatorKeyPair for main_pk \"{}\", test pk \"{}\"",
                participant.mainnet_identity, participant.testnet_identity
            )
            .into());
        }
    }
    Ok(())
}

/// returns the names for the validators
fn get_validator_names(rpc_client: &RpcClient) -> BoxResult<HashMap<Pubkey, Map<String, Value>>> {
    let mut data_map: HashMap<Pubkey, Map<_, _>> = HashMap::new();

    let all_stake_accounts = rpc_client.get_program_accounts(&solana_config_program::id())?;
    for (_, account) in all_stake_accounts {
        let key_list: ConfigKeys = deserialize(&account.data)?;

        if !key_list.keys.is_empty() {
            let (validator_pubkey, _) = key_list.keys[1];

            let validator_info_string: String = deserialize(get_config_data(&account.data)?)?;
            let validator_info: Map<_, _> = serde_json::from_str(&validator_info_string)?;
            data_map.insert(validator_pubkey, validator_info);
        }
    }

    Ok(data_map)
}

lazy_static! {
    // TODO: update yml file so that these values can be accessed without using regexes like this
    /// Regular expressions for parsing the notes
    static ref NOTE_REGEXES: Vec<(Vec<&'static str>,  Regex)> = {
        let v: Vec<(Vec<&'static str>,  Regex)> = vec![
            // Minimum vote credits required for epoch 198: 104483 (cluster average: 160744, grace: 35%)
            (vec![
                "min_vote_credits",
                "avg_vote_credits",
                "vote_credits_grace_percent"
            ], Regex::new(r"Minimum vote credits required for epoch \d+: (\d+) \(cluster average: ([\d\\.]+), grace: ([\d\\.]+)%\)").unwrap()),

            // Maximum allowed skip rate for epoch 198: 61% (cluster average: 26%, grace: 35%)
            (vec![
                "max_skip_rate",
                "avg_skip_rate",
                "skip_rate_grace"
            ], Regex::new(r"Maximum allowed skip rate for epoch \d+: ([\d+\\.]+)% \(cluster average: ([\d\\.]+)%, grace: ([\d\\.]+)%\)").unwrap()),

            // Solana release 1.7.0 or greater required
            (vec![
                "min_solana_version",
            ], Regex::new(r"Solana release (\S+) or greater required").unwrap()),

            // Maximum commission: 100%
            (vec![
                "max_commission",
            ], Regex::new(r"Maximum commission: ([\d\\.]+)%").unwrap()),

            // Minimum required self stake: ◎0.000000000
            (vec![
                "min_self_stake",
            ], Regex::new(r"Minimum required self stake: ◎([\d\\.]+)").unwrap()),

            // Maximum active stake allowed: ◎3500000.000000000
            (vec![
                "max_self_stake",
            ], Regex::new(r"Maximum active stake allowed: ◎([\d\\.]+)").unwrap()),

            // Maximum infrastructure concentration: 30%
            (vec![
                "max_infrastructure_concentration",
            ], Regex::new(r"Maximum infrastructure concentration: ([\d\\.]+)%").unwrap()),

            // 2036 validators processed
            (vec![
                "num_validators_processed",
            ], Regex::new(r"(\d+) validators processed").unwrap()),


            // Active stake: ◎77812810.078959202
            (vec![
                "active_stake",
            ], Regex::new(r"Active stake: ◎([\d\\.]+)").unwrap()),

            // Stake pool size: ◎77646465.639826297 (available for delegation: ◎3667127.924089391)
            (vec![
                "stake_pool_size",
                "stake_pool_available_for_delegation",
            ], Regex::new(r"Stake pool size: ◎([\d\\.]+) \(available for delegation: ◎([\d\\.]+)\)").unwrap()),

            // Baseline stake amount: ◎5000.000000000
            (vec![
                "baseline_stake_amount",
            ], Regex::new(r"Baseline stake amount: ◎([\d\\.]+)").unwrap()),

            // Bonus stake amount: ◎46269.599541788
            (vec![
                "bonus_stake_amount",
            ], Regex::new(r"Bonus stake amount: ◎([\d\\.]+)").unwrap()),


            // Validators by stake level: None=200, Baseline=177, Bonus=1659
            (vec![
                "num_no_stake_validators",
                "num_baseline_stake_validators",
                "num_bonus_stake_validators",
            ], Regex::new(r"Validators by stake level: None=(\d+), Baseline=(\d+), Bonus=(\d+)").unwrap()),

            (vec![
                "min_testnet_participation_numerator",
                "min_testnet_participation_denominator",
            ], Regex::new(r"Participants must maintain Baseline or Bonus stake level for (\d+) of the last (\d+) Testnet epochs").unwrap())
        ];

        v
    };
}

/// parse the notes in the yaml file to get the validator program epoch data
fn read_epoch_notes(notes: &[String]) -> Map<String, Value> {
    let mut note_vals = Map::new();

    for note in notes {
        let mut captures;

        let mut found = false;

        for (vars, regex) in NOTE_REGEXES.iter() {
            captures = regex.captures(&note);
            if let Some(cap) = captures {
                found = true;

                for (i, var) in vars.iter().enumerate() {
                    let str_value = &cap[i + 1];

                    let maybe_number: Result<Number, _> = serde_json::from_str(str_value);

                    let value: Value;
                    value = match maybe_number {
                        Ok(v) => serde_json::to_value(v).unwrap(),
                        Err(_) => serde_json::to_value(str_value).unwrap(),
                    };
                    note_vals.insert(var.to_string(), value);
                }

                break;
            }
        }
        if !found {
            warn!("Could not find match for: {}", note);
        }
    }

    note_vals
}

fn persist_epoch_notes(
    notes: &Map<String, Value>,
    transaction: &mut Transaction<'_>,
    config: &Config,
    epoch: Epoch,
) -> BoxResult<()> {
    // usize does not have toSql trait, so change to i32, which does
    let epoch_i32 = i32::try_from(epoch)?;
    let rows = transaction.query(
        r#"SELECT id from "EpochStats" WHERE epoch=$1"#,
        &[&epoch_i32],
    )?;

    let num_rows = rows.len();
    if num_rows == 1 {
        let id: i32 = rows.first().unwrap().get("id");

        transaction.execute(
            r#"UPDATE "EpochStats"
                SET epoch=$1,
                cluster=$2,
                stats=$3
                WHERE id=$4"#,
            &[
                &epoch_i32,
                &config.cluster.to_string(),
                &Json::<&Map<String, Value>>(notes),
                &id,
            ],
        )?;
    } else if num_rows == 0 {
        transaction.execute(
            r#"INSERT INTO "EpochStats"
                    (epoch, cluster, stats)
                    VALUES ($1, $2, $3)"#,
            &[
                &epoch_i32,
                &config.cluster.to_string(),
                &Json::<&Map<String, Value>>(notes),
            ],
        )?;
    } else {
        return Err("ERROR: > 1 rows for epoch".into());
    }
    Ok(())
}

fn persist_validator_stats(
    stats: HashMap<Pubkey, Map<String, Value>>,
    transaction: &mut Transaction<'_>,
    config: &Config,
    epoch: Epoch,
    validator_names: HashMap<Pubkey, Map<String, Value>>,
) -> BoxResult<()> {
    let epoch_i32 = i32::try_from(epoch)?;

    for (pk, sts) in stats.iter() {
        let rows = transaction.query(
            r#"SELECT id from "ValidatorEpochStats"
            WHERE epoch=$1 and validator_pk=$2"#,
            &[&epoch_i32, &pk.to_string()],
        );

        let rows = rows?;
        let num_rows = rows.len();

        if num_rows > 1 {
            return Err(format!(
                "Could not update EpochValidatorStats: > 1 rows for epoch {}, validator {}",
                epoch, pk
            )
            .into());
        } else if num_rows == 1 {
            let id: i32 = rows.first().unwrap().get("id");

            transaction.execute(
                r#"UPDATE "ValidatorEpochStats"
                SET epoch=$1,
                cluster=$2,
                stats=$3
                WHERE id=$4"#,
                &[
                    &epoch_i32,
                    &config.cluster.to_string(),
                    &Json::<&Map<String, Value>>(&sts),
                    &id,
                ],
            )?;
        } else if num_rows == 0 {
            transaction.execute(
                r#"INSERT INTO "ValidatorEpochStats"
                    (validator_pk, epoch, stats, cluster)
                    VALUES ($1, $2, $3, $4)"#,
                &[
                    &pk.to_string(),
                    &epoch_i32,
                    &Json::<&Map<String, Value>>(&sts),
                    &config.cluster.to_string(),
                ],
            )?;
        }

        let data = validator_names.get(pk);

        let name;
        let keybase_username;

        if let Some(data) = data {
            let default_value = &serde_json::to_value("")?;
            name = data
                .get("name")
                .unwrap_or(default_value)
                .as_str()
                .unwrap()
                .to_string();
            keybase_username = data
                .get("keybaseUsername")
                .unwrap_or(default_value)
                .as_str()
                .unwrap()
                .to_string();
        } else {
            name = "".to_string();
            keybase_username = "".to_string();
        }

        update_validator_stats(
            transaction,
            &config,
            &epoch_i32,
            sts.get("state").unwrap(),
            pk,
            &name,
            &keybase_username,
        )?;
    }

    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
struct EpochStatsStat {
    epochs: Map<String, Value>,
}

/// updates the ValidatorStats table, which holds the summary for the validator stats
fn update_validator_stats(
    transaction: &mut Transaction<'_>,
    config: &Config,
    epoch: &i32,
    status: &Value,
    public_key: &Pubkey,
    name: &str,
    keybase_username: &str,
) -> BoxResult<()> {
    trace!("Updating ValidatorStats for {}", public_key);

    // Get the table id, if it already exists
    let rows = transaction.query(
        r#"SELECT * FROM "ValidatorStats"
            WHERE cluster=$1 AND validator_pk=$2"#,
        &[&config.cluster.to_string(), &public_key.to_string()],
    )?;

    let num_rows = rows.len();
    if num_rows == 1 {
        let row = rows.first().unwrap();
        let id: i32 = row.get("id");

        let mut stats: EpochStatsStat = serde_json::from_value(row.get("stats"))?;

        stats.epochs.insert(epoch.to_string(), status.clone());

        transaction.execute(
            r#"UPDATE "ValidatorStats"
            SET stats=$1, name=$2, keybase_username=$3
            WHERE id=$4"#,
            &[
                &Json::<EpochStatsStat>(stats),
                &name.to_string(),
                &keybase_username.to_string(),
                &id,
            ],
        )?;
    } else if num_rows == 0 {
        let mut epochs = Map::new();
        epochs.insert(epoch.to_string(), status.clone());

        let stats = EpochStatsStat { epochs };

        transaction.execute(
            r#"INSERT INTO "ValidatorStats"
            (validator_pk, cluster, stats, name, keybase_username)
            VALUES
            ($1, $2, $3, $4, $5)"#,
            &[
                &public_key.to_string(),
                &config.cluster.to_string(),
                &Json::<&EpochStatsStat>(&stats),
                &name.to_string(),
                &keybase_username.to_string(),
            ],
        )?;
    } else {
        return Err(format!("More than one row in ValidatorStats for {}", public_key).into());
    }
    Ok(())
}
