use core::time::Duration;
use fuel_core::{
    chain_config::{
        CoinConfig,
        CoinConfigGenerator,
        StateConfig,
        TESTNET_WALLET_SECRETS,
    },
    fuel_core_graphql_api::{
        da_compression::{
            DbTx,
            DecompressDbTx,
        },
        worker_service::DaCompressionConfig,
    },
    p2p_test_helpers::*,
    service::{
        Config,
        FuelService,
    },
};
use fuel_core_client::client::{
    pagination::PaginationRequest,
    types::TransactionStatus,
    FuelClient,
};
use fuel_core_compression::{
    decompress::decompress,
    VersionedCompressedBlock,
};
use fuel_core_storage::transactional::{
    HistoricalView,
    IntoTransaction,
};
use fuel_core_types::{
    entities::coins::{
        coin::Coin,
        CoinType,
    },
    fuel_asm::{
        op,
        RegId,
    },
    fuel_crypto::SecretKey,
    fuel_tx::{
        Address,
        AssetId,
        ConsensusParameters,
        GasCosts,
        Input,
        Output,
        Transaction,
        TransactionBuilder,
        TxPointer,
    },
    secrecy::Secret,
    signer::SignMode,
};
use rand::{
    rngs::StdRng,
    SeedableRng,
};
use std::str::FromStr;

#[tokio::test]
async fn can_fetch_da_compressed_block_from_graphql() {
    let mut rng = StdRng::seed_from_u64(10);
    let poa_secret = SecretKey::random(&mut rng);

    let mut config = Config::local_node();
    config.consensus_signer = SignMode::Key(Secret::new(poa_secret.into()));
    config.utxo_validation = true;
    let compression_config = fuel_core_compression::Config {
        temporal_registry_retention: Duration::from_secs(3600),
    };
    config.da_compression = DaCompressionConfig::Enabled(compression_config);
    let srv = FuelService::new_node(config).await.unwrap();
    let client = FuelClient::from(srv.bound_address);

    let wallet_secret =
        SecretKey::from_str(TESTNET_WALLET_SECRETS[1]).expect("Expected valid secret");
    let wallet_address = Address::from(*wallet_secret.public_key().hash());

    let coin = client
        .coins(
            &wallet_address,
            None,
            PaginationRequest {
                cursor: None,
                results: 1,
                direction: fuel_core_client::client::pagination::PageDirection::Forward,
            },
        )
        .await
        .expect("Unable to get coins")
        .results
        .into_iter()
        .next()
        .expect("Expected at least one coin");

    let tx =
        TransactionBuilder::script([op::ret(RegId::ONE)].into_iter().collect(), vec![])
            .max_fee_limit(0)
            .script_gas_limit(1_000_000)
            .with_gas_costs(GasCosts::free())
            .add_unsigned_coin_input(
                wallet_secret,
                coin.utxo_id,
                coin.amount,
                coin.asset_id,
                TxPointer::new(coin.block_created.into(), coin.tx_created_idx),
            )
            .finalize_as_transaction();

    let status = client.submit_and_await_commit(&tx).await.unwrap();

    let block_height = match status {
        TransactionStatus::Success { block_height, .. } => block_height,
        other => {
            panic!("unexpected result {other:?}")
        }
    };

    let block = client
        .da_compressed_block(block_height)
        .await
        .unwrap()
        .expect("Unable to get compressed block");
    let block: VersionedCompressedBlock = postcard::from_bytes(&block).unwrap();

    // Reuse the existing offchain db to decompress the block
    let db = &srv.shared.database;
    let mut tx_inner = db.off_chain().clone().into_transaction();
    let db_tx = DecompressDbTx {
        db_tx: DbTx {
            db_tx: &mut tx_inner,
        },
        onchain_db: db.on_chain().view_at(&0u32.into()).unwrap(),
    };
    let decompressed = decompress(compression_config, db_tx, block).await.unwrap();

    assert!(decompressed.transactions.len() == 2);
}

#[tokio::test]
async fn foo() {
    let mut rng = StdRng::seed_from_u64(10);
    let poa_secret = SecretKey::random(&mut rng);

    let mut config = Config::local_node();
    config.consensus_signer = SignMode::Key(Secret::new(poa_secret.into()));
    config.utxo_validation = true;
    config.debug = true;

    let alice_secret = TESTNET_WALLET_SECRETS[0];
    let bob_secret = TESTNET_WALLET_SECRETS[1];
    assert_ne!(alice_secret, bob_secret);

    let alice_wallet_secret =
        SecretKey::from_str(alice_secret).expect("Expected valid secret");
    let bob_wallet_secret =
        SecretKey::from_str(bob_secret).expect("Expected valid secret");
    assert_ne!(alice_wallet_secret, bob_wallet_secret);

    let alice_wallet_address = Address::from(*alice_wallet_secret.public_key().hash());
    let bob_wallet_address = Address::from(*bob_wallet_secret.public_key().hash());

    assert_ne!(alice_wallet_address, bob_wallet_address);

    let coin: CoinConfig = {
        // setup all coins for all owners
        let mut coin_generator = CoinConfigGenerator::new();
        CoinConfig {
            owner: alice_wallet_address,
            amount: 200_000_000,
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

    let srv = FuelService::new_node(config).await.unwrap();
    let client = FuelClient::from(srv.bound_address);
    let consensus_parameters = client.consensus_parameters(0).await.unwrap().unwrap();

    let alice_coins: Vec<_> = client
        .coins(
            &alice_wallet_address,
            None,
            PaginationRequest {
                cursor: None,
                results: 10,
                direction: fuel_core_client::client::pagination::PageDirection::Forward,
            },
        )
        .await
        .expect("Unable to get coins")
        .results
        .into_iter()
        .collect();
    dbg!(&alice_coins);
    let bob_coins: Vec<_> = client
        .coins(
            &bob_wallet_address,
            None,
            PaginationRequest {
                cursor: None,
                results: 10,
                direction: fuel_core_client::client::pagination::PageDirection::Forward,
            },
        )
        .await
        .expect("Unable to get coins")
        .results
        .into_iter()
        .collect();
    dbg!(&bob_coins);

    let alice_coin = alice_coins.first().expect("should have one coin");
    dbg!(&alice_coin);

    /// Creates the transfer transaction.
    pub fn transfer_tx(
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

    // let tx =
    //     TransactionBuilder::script([op::ret(RegId::ONE)].into_iter().collect(), vec![])
    //         .max_fee_limit(0)
    //         .script_gas_limit(1_000_000)
    //         .with_gas_costs(GasCosts::free())
    //         .add_unsigned_coin_input(
    //             alice_wallet_secret,
    //             alice_coin.utxo_id,
    //             alice_coin.amount,
    //             alice_coin.asset_id,
    //             TxPointer::new(alice_coin.block_created.into(), alice_coin.tx_created_idx),
    //         )
    //         .finalize_as_transaction();

    let tx = transfer_tx(
        alice_wallet_address,
        bob_wallet_address,
        &alice_wallet_secret,
        &alice_coin,
        100_000_000,
        consensus_parameters.clone(),
    );

    let tx = tx.unwrap();
    dbg!(&tx);

    let height_before_transfer = client
        .chain_info()
        .await
        .unwrap()
        .latest_block
        .header
        .height;

    dbg!(&height_before_transfer);

    let status = client.submit_and_await_commit(&tx).await.unwrap();
    dbg!(&status);

    let block_height = match status {
        TransactionStatus::Success { block_height, .. } => block_height,
        other => {
            panic!("unexpected result {other:?}")
        }
    };

    let height_after_transfer = *block_height;

    dbg!(&height_after_transfer);

    assert!(
        height_after_transfer > height_before_transfer,
        "block height should increase"
    );

    let alice_coins_after_transaction: Vec<_> = client
        .coins(
            &alice_wallet_address,
            None,
            PaginationRequest {
                cursor: None,
                results: 10,
                direction: fuel_core_client::client::pagination::PageDirection::Forward,
            },
        )
        .await
        .expect("Unable to get coins")
        .results
        .into_iter()
        .collect();
    dbg!(&alice_coins_after_transaction);
    let bob_coins_after_transaction: Vec<_> = client
        .coins(
            &bob_wallet_address,
            None,
            PaginationRequest {
                cursor: None,
                results: 10,
                direction: fuel_core_client::client::pagination::PageDirection::Forward,
            },
        )
        .await
        .expect("Unable to get coins")
        .results
        .into_iter()
        .collect();
    dbg!(&bob_coins_after_transaction);

    let tx = transfer_tx(
        alice_wallet_address,
        bob_wallet_address,
        &alice_wallet_secret,
        &alice_coin,
        150_000_000,
        consensus_parameters.clone(),
    )
    .unwrap();
    println!("dry running on block {height_after_transfer}");
    let status = client
        .dry_run_opt(&[tx], Some(true), None, Some(height_after_transfer))
        .await
        .unwrap();
    let st1 = status.first().unwrap();
    dbg!(&st1);

    let tx = transfer_tx(
        alice_wallet_address,
        bob_wallet_address,
        &alice_wallet_secret,
        &alice_coin,
        150_000_000,
        consensus_parameters,
    )
    .unwrap();
    println!("dry running on block {height_before_transfer}");
    let status = client
        .dry_run_opt(&[tx], Some(true), None, Some(height_before_transfer))
        .await;
    dbg!(&status);

    // let st2 = status.first().unwrap();
    // dbg!(&st2);

    panic!();

    let block = client
        .da_compressed_block(block_height)
        .await
        .unwrap()
        .expect("Unable to get compressed block");
    let block: VersionedCompressedBlock = postcard::from_bytes(&block).unwrap();

    // Reuse the existing offchain db to decompress the block
    let db = &srv.shared.database;
    let mut tx_inner = db.off_chain().clone().into_transaction();
    let db_tx = DecompressDbTx {
        db_tx: DbTx {
            db_tx: &mut tx_inner,
        },
        onchain_db: db.on_chain().view_at(&0u32.into()).unwrap(),
    };
}

#[tokio::test(flavor = "multi_thread")]
async fn da_compressed_blocks_are_available_from_non_block_producing_nodes() {
    let mut rng = StdRng::seed_from_u64(line!() as u64);

    // Create a producer and a validator that share the same key pair.
    let secret = SecretKey::random(&mut rng);
    let pub_key = Input::owner(&secret.public_key());

    let mut config = Config::local_node();
    config.da_compression = DaCompressionConfig::Enabled(fuel_core_compression::Config {
        temporal_registry_retention: Duration::from_secs(3600),
    });

    let Nodes {
        mut producers,
        mut validators,
        bootstrap_nodes: _dont_drop,
    } = make_nodes(
        [Some(BootstrapSetup::new(pub_key))],
        [Some(
            ProducerSetup::new(secret).with_txs(1).with_name("Alice"),
        )],
        [Some(ValidatorSetup::new(pub_key).with_name("Bob"))],
        Some(config),
    )
    .await;

    let producer = producers.pop().unwrap();
    let mut validator = validators.pop().unwrap();

    let v_client = FuelClient::from(validator.node.shared.graph_ql.bound_address);

    // Insert some txs
    let expected = producer.insert_txs().await;
    validator.consistency_20s(&expected).await;

    let block_height = 1u32.into();

    let block = v_client
        .da_compressed_block(block_height)
        .await
        .unwrap()
        .expect("Compressed block not available from validator");
    let _: VersionedCompressedBlock = postcard::from_bytes(&block).unwrap();
}
