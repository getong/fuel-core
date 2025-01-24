#![allow(non_snake_case)]

use clap::Parser;
use fuel_core::{
    combined_database::CombinedDatabase,
    service::{
        config::fuel_core_importer::ports::Validator,
        FuelService,
    },
};
use fuel_core_client::client::FuelClient;
use fuel_core_storage::transactional::AtomicView;
use fuel_core_types::{
    fuel_tx::{
        AssetId,
        Input,
        Output,
        Transaction,
        TransactionBuilder,
        TxId,
        UniqueIdentifier,
    },
    fuel_types::BlockHeight,
};
use futures::StreamExt;
use itertools::Itertools;
use rand::{
    prelude::StdRng,
    Rng,
    SeedableRng,
};
use std::collections::BTreeSet;
use tempfile::TempDir;
use test_helpers::{
    fuel_core_driver::FuelCoreDriver,
    produce_block_with_tx,
};

fn transfer_transaction(min_amount: u64, rng: &mut StdRng) -> Transaction {
    let mut builder = TransactionBuilder::script(vec![], vec![]);

    let number_of_inputs = rng.gen_range(1..10);

    for _ in 0..number_of_inputs {
        let utxo_id = rng.gen();
        let owner: [u8; 32] = [rng.gen_range(0..10); 32];
        let amount = rng.gen_range(min_amount..min_amount + 100_000);
        builder.add_input(Input::coin_predicate(
            utxo_id,
            owner.into(),
            amount,
            AssetId::BASE,
            Default::default(),
            0,
            vec![0],
            vec![],
        ));
    }

    for _ in 0..number_of_inputs {
        let owner: [u8; 32] = [rng.gen_range(0..10); 32];
        builder.add_output(Output::coin(owner.into(), min_amount, AssetId::BASE));
    }

    builder.finalize_as_transaction()
}

#[tokio::test(flavor = "multi_thread")]
async fn validate_block_at_any_height__only_transfers() -> anyhow::Result<()> {
    let mut rng = StdRng::seed_from_u64(1234);
    let driver = FuelCoreDriver::spawn_feeless(&[
        "--debug",
        "--poa-instant",
        "true",
        "--state-rewind-duration",
        "7d",
    ])
    .await?;
    let node = &driver.node;

    // Given
    const TOTAL_BLOCKS: u64 = 1000;
    const MIN_AMOUNT: u64 = 123456;
    let mut last_block_height = 0u32;
    let mut database_modifications = std::collections::HashMap::new();
    for _ in 0..TOTAL_BLOCKS {
        let mut blocks = node.shared.block_importer.events();
        let tx = transfer_transaction(MIN_AMOUNT, &mut rng);
        let result = node.submit_and_await_commit(tx).await.unwrap();
        assert!(matches!(
            result,
            fuel_core::schema::tx::types::TransactionStatus::Success(_)
        ));

        let block = blocks.next().await.unwrap();
        let block_height = *block.shared_result.sealed_block.entity.header().height();
        last_block_height = block_height.into();
        database_modifications.insert(last_block_height, block.changes.as_ref().clone());
    }

    let view = node.shared.database.on_chain().latest_view().unwrap();
    for i in 0..TOTAL_BLOCKS {
        let height_to_execute = rng.gen_range(1..last_block_height);

        let block = view
            .get_full_block(&height_to_execute.into())
            .unwrap()
            .unwrap();

        // When
        tracing::info!("Validating block {i} at height {}", height_to_execute);
        let result = node.shared.executor.validate(&block);
        let client = FuelClient::from(node.bound_address);
        let tx = block.transactions()[0].clone();
        let dry_run_result = client
            .dry_run_opt(&[tx], None, None, Some(height_to_execute))
            .await;

        // Then
        let _ = dry_run_result.expect("Dry run should succeed");
        let height_to_execute: BlockHeight = height_to_execute.into();
        let result = result.unwrap();
        let expected_changes = database_modifications.get(&height_to_execute).unwrap();
        let actual_changes = result.into_changes();
        assert_eq!(&actual_changes, expected_changes);
    }

    driver.kill().await;
    Ok(())
}

fn all_real_transactions(node: &FuelService) -> BTreeSet<TxId> {
    node.shared
        .database
        .on_chain()
        .all_transactions(None, None)
        .filter_map(|tx| match tx {
            Ok(tx) => {
                if tx.is_mint() {
                    None
                } else {
                    Some(Ok(tx.id(&Default::default())))
                }
            }
            Err(err) => Some(Err(err)),
        })
        .try_collect()
        .unwrap()
}

async fn rollback_existing_chain_to_target_height_and_verify(
    target_height: u32,
    blocks_in_the_chain: u32,
) -> anyhow::Result<()> {
    let mut rng = StdRng::seed_from_u64(1234);
    let driver = FuelCoreDriver::spawn_feeless(&[
        "--debug",
        "--poa-instant",
        "true",
        "--state-rewind-duration",
        "7d",
    ])
    .await?;
    let node = &driver.node;

    // Given
    const MIN_AMOUNT: u64 = 123456;
    let mut transactions = vec![];
    for _ in 0..blocks_in_the_chain {
        let tx = transfer_transaction(MIN_AMOUNT, &mut rng);
        transactions.push(tx.id(&Default::default()));

        let result = node.submit_and_await_commit(tx).await.unwrap();
        assert!(
            matches!(
                result,
                fuel_core::schema::tx::types::TransactionStatus::Success(_)
            ),
            "Transaction got unexpected status {:?}",
            result
        );
    }
    let all_transactions = all_real_transactions(node);
    assert_eq!(all_transactions.len(), blocks_in_the_chain as usize);

    // When
    let temp_dir = driver.kill().await;
    let target_block_height = target_height.to_string();
    let args = [
        "_IGNORED_",
        "--db-path",
        temp_dir.path().to_str().unwrap(),
        "--target-block-height",
        target_block_height.as_str(),
    ];
    let command = fuel_core_bin::cli::rollback::Command::parse_from(args);
    tracing::info!("Rolling back to block {}", target_block_height);
    fuel_core_bin::cli::rollback::exec(command).await?;

    // Then
    let driver = FuelCoreDriver::spawn_feeless_with_directory(
        temp_dir,
        &[
            "--debug",
            "--poa-instant",
            "true",
            "--state-rewind-duration",
            "7d",
        ],
    )
    .await?;
    let node = &driver.node;
    let remaining_transactions = all_real_transactions(node);

    let expected_transactions = transactions
        .into_iter()
        .take(target_height as usize)
        .collect::<BTreeSet<_>>();
    assert_eq!(remaining_transactions.len(), expected_transactions.len());
    pretty_assertions::assert_eq!(remaining_transactions, expected_transactions);

    let latest_height = node
        .shared
        .database
        .on_chain()
        .latest_height_from_metadata();
    assert_eq!(Ok(Some(BlockHeight::new(target_height))), latest_height);

    driver.kill().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_chain_to_genesis() -> anyhow::Result<()> {
    let genesis_block_height = 0;
    let blocks_in_the_chain = 100;
    rollback_existing_chain_to_target_height_and_verify(
        genesis_block_height,
        blocks_in_the_chain,
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_chain_to_middle() -> anyhow::Result<()> {
    let target_rollback_block_height = 50;
    let blocks_in_the_chain = 100;
    rollback_existing_chain_to_target_height_and_verify(
        target_rollback_block_height,
        blocks_in_the_chain,
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_chain_to_same_height() -> anyhow::Result<()> {
    let target_rollback_block_height = 100;
    let blocks_in_the_chain = 100;
    rollback_existing_chain_to_target_height_and_verify(
        target_rollback_block_height,
        blocks_in_the_chain,
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_chain_to_same_height_1000() -> anyhow::Result<()> {
    let target_rollback_block_height = 800;
    let blocks_in_the_chain = 1000;
    rollback_existing_chain_to_target_height_and_verify(
        target_rollback_block_height,
        blocks_in_the_chain,
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_to__should_work_with_empty_gas_price_database() -> anyhow::Result<()> {
    let mut rng = StdRng::seed_from_u64(1234);
    let driver = FuelCoreDriver::spawn_feeless(&[
        "--debug",
        "--poa-instant",
        "true",
        "--state-rewind-duration",
        "7d",
    ])
    .await?;

    // Given
    const TOTAL_BLOCKS: u64 = 500;
    for _ in 0..TOTAL_BLOCKS {
        produce_block_with_tx(&mut rng, &driver.client).await;
    }
    let temp_dir = driver.kill().await;
    std::fs::remove_dir_all(temp_dir.path().join("gas_price")).unwrap();

    // When
    let args = [
        "_IGNORED_",
        "--db-path",
        temp_dir.path().to_str().unwrap(),
        "--target-block-height",
        "1",
    ];
    let command = fuel_core_bin::cli::rollback::Command::parse_from(args);
    let result = fuel_core_bin::cli::rollback::exec(command).await;

    // Then
    result.expect("Rollback should succeed");

    Ok(())
}

// #[tokio::test(flavor = "multi_thread")]
// async fn validate_block_at_any_height__mainnet() -> anyhow::Result<()> {
// let args = [
// "_IGNORED_",
// "--port",
// "0",
// "--debug",
// "--poa-instant",
// "false",
// "--utxo-validation",
// "--state-rewind-duration",
// "136y",
// "--db-path",
// "/Users/green/.fuel-mainnet-3",
// "--snapshot",
// "/Users/green/fuel/chain-configuration/ignition",
// ];
//
// let node = fuel_core_bin::cli::run::get_service(
// fuel_core_bin::cli::run::Command::parse_from(args),
// )
// .await?;
//
// node.start_and_await().await?;
//
// Given
// let view = node.shared.database.on_chain().latest_view().unwrap();
// let height_to_execute = 6006774u32.into();
//
// When
// let block = view.get_full_block(&height_to_execute).unwrap().unwrap();
//
// Then
// tracing::info!("Validating block at height {}", *height_to_execute);
//
// const CONTRACT: &[u8] = &[0, 0, 0];
// const CONTRACT: &[u8] = include_bytes!("/Users/green/Downloads/market_fixed.bin");
//
// let contract_id: ContractId =
// "0x657ab45a6eb98a4893a99fd104347179151e8b3828fd8f2a108cc09770d1ebae"
// .parse()
// .unwrap();
//
// let mut tx = view.read_transaction();
// tx.storage_as_mut::<ContractsRawCode>()
// .insert(&contract_id, CONTRACT)
// .unwrap();
//
// let overrides = Overrides {
// on_chain_changes: tx.into_changes(),
// };
//
// let result = node
// .shared
// .executor
// .validate_with_overrides(&block, Some(overrides));
// tracing::info!("Result: {:?}", result);
// }

async fn backup_and_restore__should_work_with_state_rewind() -> anyhow::Result<()> {
    let mut rng = StdRng::seed_from_u64(1234);
    let driver = FuelCoreDriver::spawn_feeless(&[
        "--debug",
        "--poa-instant",
        "true",
        "--state-rewind-duration",
        "7d",
    ])
    .await?;
    let node = &driver.node;

    // Given
    // setup a node, produce some blocks
    const TOTAL_BLOCKS: u64 = 20;
    const MIN_AMOUNT: u64 = 123456;
    let mut last_block_height = 0u32;
    let mut database_modifications = std::collections::HashMap::new();
    for _ in 0..TOTAL_BLOCKS {
        let mut blocks = node.shared.block_importer.events();
        let tx = transfer_transaction(MIN_AMOUNT, &mut rng);
        let result = node.submit_and_await_commit(tx).await.unwrap();
        assert!(matches!(
            result,
            fuel_core::schema::tx::types::TransactionStatus::Success(_)
        ));

        let block = blocks.next().await.unwrap();
        let block_height = *block.shared_result.sealed_block.entity.header().height();
        last_block_height = block_height.into();
        database_modifications.insert(last_block_height, block.changes.as_ref().clone());
    }

    // create a backup and delete the database
    let db_dir = driver.kill().await;
    let backup_dir = TempDir::new().unwrap();
    CombinedDatabase::backup(db_dir.path(), backup_dir.path()).unwrap();
    drop(db_dir);

    // restore the backup
    let new_db_dir = TempDir::new().unwrap();
    CombinedDatabase::restore(new_db_dir.path(), backup_dir.path()).unwrap();

    // start the node again, with the new db
    let driver = FuelCoreDriver::spawn_feeless_with_directory(
        new_db_dir,
        &[
            "--debug",
            "--poa-instant",
            "true",
            "--state-rewind-duration",
            "7d",
        ],
    )
    .await
    .unwrap();
    let node = &driver.node;

    let view = node.shared.database.on_chain().latest_view().unwrap();

    for i in 0..TOTAL_BLOCKS {
        let height_to_execute = rng.gen_range(1..last_block_height);

        let block = view
            .get_full_block(&height_to_execute.into())
            .unwrap()
            .unwrap();

        // When
        tracing::info!("Validating block {i} at height {}", height_to_execute);
        let result = node.shared.executor.validate(&block);

        // Then
        let height_to_execute: BlockHeight = height_to_execute.into();
        let result = result.unwrap();
        let expected_changes = database_modifications.get(&height_to_execute).unwrap();
        let actual_changes = result.into_changes();
        assert_eq!(&actual_changes, expected_changes);
    }

    driver.kill().await;
    Ok(())
}

mod dry_run_transaction_in_the_past {
    use std::{
        collections::BTreeSet,
        str::FromStr,
    };

    use fuel_core::{
        chain_config::{
            CoinConfig,
            CoinConfigGenerator,
            StateConfig,
            TESTNET_WALLET_SECRETS,
        },
        service::Config,
    };
    use fuel_core_bin::FuelService;
    use fuel_core_client::client::{
        pagination::PaginationRequest,
        types::Coin,
        FuelClient,
    };
    use fuel_core_types::{
        fuel_tx::{
            Address,
            AssetId,
            ConsensusParameters,
            Output,
            Transaction,
            TransactionBuilder,
        },
        fuel_vm::SecretKey,
    };

    #[tokio::test]
    async fn can_dry_run_transaction_in_the_past() {
        const INITIAL_BALANCE: u64 = 300_000_000;

        let (alice, bob) = setup_actors();
        let config = setup_config_with_coin(&alice, INITIAL_BALANCE);

        let srv = FuelService::new_node(config).await.unwrap();
        let client = FuelClient::from(srv.bound_address);

        assert_coins(&client, &alice, &[INITIAL_BALANCE]).await;
        assert_coins(&client, &bob, &[]).await;

        transfer_and_check_dry_run(&client, &alice, &bob, 100_000_000).await;
        assert_coins(&client, &alice, &[200_000_000]).await;
        assert_coins(&client, &bob, &[100_000_000]).await;

        transfer_and_check_dry_run(&client, &alice, &bob, 100_000_000).await;
        assert_coins(&client, &alice, &[100_000_000]).await;
        assert_coins(&client, &bob, &[100_000_000, 100_000_000]).await;
    }

    struct Actor {
        secret: SecretKey,
    }

    impl Actor {
        fn secret(&self) -> &SecretKey {
            &self.secret
        }

        fn address(&self) -> Address {
            let hash = self.secret.public_key().hash();
            Address::from(*hash)
        }
    }

    fn setup_actors() -> (Actor, Actor) {
        let alice_secret = TESTNET_WALLET_SECRETS[0];
        let bob_secret = TESTNET_WALLET_SECRETS[1];
        assert_ne!(alice_secret, bob_secret);

        let alice_wallet_secret =
            SecretKey::from_str(alice_secret).expect("Expected valid secret");
        let bob_wallet_secret =
            SecretKey::from_str(bob_secret).expect("Expected valid secret");
        assert_ne!(alice_wallet_secret, bob_wallet_secret);

        (
            Actor {
                secret: alice_wallet_secret,
            },
            Actor {
                secret: bob_wallet_secret,
            },
        )
    }

    fn setup_config_with_coin(actor: &Actor, amount: u64) -> Config {
        let coin: CoinConfig = {
            // setup all coins for all owners
            let mut coin_generator = CoinConfigGenerator::new();
            CoinConfig {
                owner: actor.address(),
                amount,
                asset_id: AssetId::default(),
                ..coin_generator.generate()
            }
        };

        let state_config = StateConfig {
            contracts: vec![],
            coins: vec![coin],
            messages: vec![],
            ..Default::default()
        };
        let mut config = Config::local_node_with_state_config(state_config);
        config.utxo_validation = true;
        config.debug = true;
        config
    }

    async fn assert_coins(client: &FuelClient, actor: &Actor, expected: &[u64]) {
        let actual: BTreeSet<_> = actor_coins(client, actor)
            .await
            .into_iter()
            .map(|coin| coin.amount)
            .collect();

        let expected: BTreeSet<_> = expected.iter().cloned().collect();
        assert_eq!(expected, actual);
    }

    async fn actor_coins(client: &FuelClient, actor: &Actor) -> Vec<Coin> {
        client
            .coins(
                &actor.address(),
                None,
                PaginationRequest {
                    cursor: None,
                    results: 10,
                    direction:
                        fuel_core_client::client::pagination::PageDirection::Forward,
                },
            )
            .await
            .expect("Unable to get coins")
            .results
            .into_iter()
            .collect()
    }

    /// Creates the transfer transaction.
    // TODO[RC]: Reuse already existing function
    pub fn transfer_tx_xxx(
        source: Address,
        destination: Address,
        secret: &SecretKey,
        coin: &fuel_core_client::client::types::Coin,
        transfer_amount: u64,
        consensus_parameters: ConsensusParameters,
    ) -> anyhow::Result<Transaction> {
        let mut tx = TransactionBuilder::script(Default::default(), Default::default());
        tx.max_fee_limit(10_000_000);
        tx.script_gas_limit(0);

        tx.add_unsigned_coin_input(
            *secret,
            coin.utxo_id,
            coin.amount,
            coin.asset_id,
            Default::default(),
        );

        tx.add_output(Output::Coin {
            to: destination,
            amount: transfer_amount,
            asset_id: coin.asset_id,
        });
        tx.add_output(Output::Change {
            to: source,
            amount: 0,
            asset_id: coin.asset_id,
        });
        tx.with_params(consensus_parameters);

        Ok(tx.finalize_as_transaction())
    }

    async fn transfer_and_assert(
        client: &FuelClient,
        from: &Actor,
        to: &Actor,
        coin: &Coin,
        amount: u64,
        expected: &str,
    ) {
        let tx = transfer_tx_xxx(
            from.address(),
            to.address(),
            from.secret(),
            coin,
            amount,
            client.consensus_parameters(0).await.unwrap().unwrap(),
        )
        .unwrap();

        let status = client.submit_and_await_commit(&tx).await.unwrap();
        let actual = format!("{:?}", status);
        assert!(
            actual.contains(expected),
            "expected: {:?}, actual: {:?}",
            expected,
            actual
        );
    }

    async fn dry_run_transfer_and_assert(
        client: &FuelClient,
        from: &Actor,
        to: &Actor,
        coin: &Coin,
        amount: u64,
        height: Option<u32>,
        expected: &str,
    ) {
        let tx = transfer_tx_xxx(
            from.address(),
            to.address(),
            from.secret(),
            coin,
            amount,
            client.consensus_parameters(0).await.unwrap().unwrap(),
        )
        .unwrap();

        let status = client.dry_run_opt(&[tx.clone()], None, None, height).await;
        let actual = format!("{:?}", status);
        assert!(
            actual.contains(expected),
            "expected: {:?}, actual: {:?}",
            expected,
            actual
        );
    }

    async fn block_height(client: &FuelClient) -> u32 {
        client
            .chain_info()
            .await
            .expect("should get chain info")
            .latest_block
            .header
            .height
    }

    // This is a helper function which does the following:
    // - Execute regular transfer
    // - Try to dry run the same transfer on the previous block and expect success
    // - Try to dry run the same transfer on the tip of the blockchain and expect failure
    async fn transfer_and_check_dry_run(
        client: &FuelClient,
        from: &Actor,
        to: &Actor,
        amount: u64,
    ) -> () {
        let from_coins = actor_coins(&client, from).await;
        let from_coin = from_coins.first().expect("should have at least one coin");

        let pre_height = block_height(&client).await;
        transfer_and_assert(&client, from, to, &from_coin, amount, "Success").await;
        let post_height = block_height(&client).await;
        assert!(post_height > pre_height);

        // Dry running on "post_height" means:
        // - revert state to "post_height - 1"
        // - apply the transaction
        // This should always succeed, as the transaction is valid at "post_height - 1",
        // because it has just been executed correctly above.
        dry_run_transfer_and_assert(
            client,
            from,
            to,
            &from_coin,
            amount,
            Some(post_height),
            "Success",
        )
        .await;

        // Dry running on the tip of the blockchain should always fail, as we cannot
        // re-run the same transfer.
        dry_run_transfer_and_assert(
            client,
            from,
            to,
            &from_coin,
            amount,
            None,
            "Response errors; Transaction id was already used",
        )
        .await;

        // This should have been an equivalent to the call above, because:
        // - `None` above means: just produce next block
        // - `Some(post_height + 1)` means: produce a block with a height equivalent to expected
        //                                  height of the next block.
        // It behaves differently, though - but maybe it's ok.
        dry_run_transfer_and_assert(
            client,
            from,
            to,
            &from_coin,
            amount,
            Some(post_height + 1),
            "Response errors; resource was not found in table",
        )
        .await;
    }
}
