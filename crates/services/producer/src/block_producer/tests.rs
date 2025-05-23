#![allow(non_snake_case)]

use crate::{
    Config,
    Producer,
    block_producer::{
        Bytes32,
        Error,
        gas_price::{
            GasPriceProvider,
            MockChainStateInfoProvider,
        },
    },
    mocks::{
        FailingMockExecutor,
        MockDb,
        MockExecutor,
        MockExecutorWithCapture,
        MockRelayer,
        MockTxPool,
    },
};
use fuel_core_producer as _;
use fuel_core_types::{
    blockchain::{
        block::{
            Block,
            CompressedBlock,
            PartialFuelBlock,
        },
        header::{
            ApplicationHeader,
            ConsensusHeader,
            PartialBlockHeader,
        },
        primitives::DaBlockHeight,
    },
    fuel_tx,
    fuel_tx::{
        ConsensusParameters,
        Mint,
        Script,
        Transaction,
        field::InputContract,
    },
    fuel_types::BlockHeight,
    services::executor::Error as ExecutorError,
    tai64::Tai64,
};
use rand::{
    Rng,
    SeedableRng,
    rngs::StdRng,
};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        Mutex,
    },
};

pub struct MockProducerGasPrice {
    pub gas_price: Option<u64>,
}

impl MockProducerGasPrice {
    pub fn new(gas_price: Option<u64>) -> Self {
        Self { gas_price }
    }
}

impl GasPriceProvider for MockProducerGasPrice {
    fn production_gas_price(&self) -> anyhow::Result<u64> {
        self.gas_price
            .ok_or_else(|| anyhow::anyhow!("Gas price not provided"))
    }

    fn dry_run_gas_price(&self) -> anyhow::Result<u64> {
        self.gas_price
            .ok_or_else(|| anyhow::anyhow!("Gas price not provided"))
    }
}

// Tests for the `produce_and_execute_block_txpool` method.
mod produce_and_execute_block_txpool {
    use super::*;
    use fuel_core_types::blockchain::primitives::DaBlockHeight;

    #[tokio::test]
    async fn cant_produce_at_genesis_height() {
        let ctx = TestContext::default();
        let producer = ctx.producer();

        let err = producer
            .produce_and_execute_block_txpool(0u32.into(), Tai64::now(), ())
            .await
            .expect_err("expected failure");

        assert!(
            matches!(
                err.downcast_ref::<Error>(),
                Some(Error::BlockHeightShouldBeHigherThanPrevious { .. })
            ),
            "unexpected err {err:?}"
        );
    }

    #[tokio::test]
    async fn can_produce_initial_block() {
        let ctx = TestContext::default();
        let producer = ctx.producer();

        let result = producer
            .produce_and_execute_block_txpool(1u32.into(), Tai64::now(), ())
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn can_produce_next_block() {
        // simple happy path for producing atop pre-existing block
        let mut rng = StdRng::seed_from_u64(0u64);
        let consensus_parameters_version = 0;
        let state_transition_bytecode_version = 0;
        // setup dummy previous block
        let prev_height = 1u32.into();
        let previous_block = PartialFuelBlock {
            header: PartialBlockHeader {
                consensus: ConsensusHeader {
                    height: prev_height,
                    prev_root: rng.r#gen(),
                    ..Default::default()
                },
                ..Default::default()
            },
            transactions: vec![],
        }
        .generate(
            &[],
            Default::default(),
            #[cfg(feature = "fault-proving")]
            &Default::default(),
        )
        .unwrap()
        .compress(&Default::default());

        let db = MockDb {
            blocks: Arc::new(Mutex::new(
                vec![(prev_height, previous_block)].into_iter().collect(),
            )),
            consensus_parameters_version,
            state_transition_bytecode_version,
        };

        let ctx = TestContext::default_from_db(db);
        let producer = ctx.producer();
        let result = producer
            .produce_and_execute_block_txpool(
                prev_height
                    .succ()
                    .expect("The block height should be valid"),
                Tai64::now(),
                (),
            )
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn next_block_contains_expected_consensus_parameters_version() {
        let mut rng = StdRng::seed_from_u64(0u64);
        // setup dummy previous block
        let prev_height = 1u32.into();
        let previous_block = PartialFuelBlock {
            header: PartialBlockHeader {
                consensus: ConsensusHeader {
                    height: prev_height,
                    prev_root: rng.r#gen(),
                    ..Default::default()
                },
                ..Default::default()
            },
            transactions: vec![],
        }
        .generate(
            &[],
            Default::default(),
            #[cfg(feature = "fault-proving")]
            &Default::default(),
        )
        .unwrap()
        .compress(&Default::default());

        // Given
        let consensus_parameters_version = 123;
        let db = MockDb {
            blocks: Arc::new(Mutex::new(
                vec![(prev_height, previous_block)].into_iter().collect(),
            )),
            consensus_parameters_version,
            state_transition_bytecode_version: 0,
        };

        let ctx = TestContext::default_from_db(db);
        let producer = ctx.producer();

        // When
        let result = producer
            .produce_and_execute_block_txpool(
                prev_height
                    .succ()
                    .expect("The block height should be valid"),
                Tai64::now(),
                (),
            )
            .await
            .expect("Should produce next block successfully")
            .into_result();

        // Then
        let header = result.block.header();
        assert_eq!(
            header.consensus_parameters_version(),
            consensus_parameters_version
        );
    }

    #[tokio::test]
    async fn next_block_contains_expected_state_transition_bytecode_version() {
        let mut rng = StdRng::seed_from_u64(0u64);
        // setup dummy previous block
        let prev_height = 1u32.into();
        let previous_block = PartialFuelBlock {
            header: PartialBlockHeader {
                consensus: ConsensusHeader {
                    height: prev_height,
                    prev_root: rng.r#gen(),
                    ..Default::default()
                },
                ..Default::default()
            },
            transactions: vec![],
        }
        .generate(
            &[],
            Default::default(),
            #[cfg(feature = "fault-proving")]
            &Default::default(),
        )
        .unwrap()
        .compress(&Default::default());

        // Given
        let state_transition_bytecode_version = 321;
        let db = MockDb {
            blocks: Arc::new(Mutex::new(
                vec![(prev_height, previous_block)].into_iter().collect(),
            )),
            consensus_parameters_version: 0,
            state_transition_bytecode_version,
        };

        let ctx = TestContext::default_from_db(db);
        let producer = ctx.producer();

        // When
        let result = producer
            .produce_and_execute_block_txpool(
                prev_height
                    .succ()
                    .expect("The block height should be valid"),
                Tai64::now(),
                (),
            )
            .await
            .expect("Should produce next block successfully")
            .into_result();

        // Then
        let header = result.block.header();
        assert_eq!(
            header.state_transition_bytecode_version(),
            state_transition_bytecode_version
        );
    }

    #[tokio::test]
    async fn cant_produce_if_no_previous_block() {
        // fail if there is no block that precedes the current height.
        let ctx = TestContext::default();
        let producer = ctx.producer();

        let err = producer
            .produce_and_execute_block_txpool(100u32.into(), Tai64::now(), ())
            .await
            .expect_err("expected failure");

        assert!(err.to_string().contains("Didn't find block for test"));
    }

    #[tokio::test]
    async fn can_produce_if_previous_block_da_height_not_changed() {
        // Given
        let da_height = DaBlockHeight(100u64);
        let prev_height = 1u32.into();
        let ctx = TestContextBuilder::new()
            .with_latest_da_block_height_from_relayer(da_height)
            .with_prev_da_height(da_height)
            .with_prev_height(prev_height)
            .build();
        let producer = ctx.producer();

        // When
        let result = producer
            .produce_and_execute_block_txpool(
                prev_height
                    .succ()
                    .expect("The block height should be valid"),
                Tai64::now(),
                (),
            )
            .await;

        // Then
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn cant_produce_if_previous_block_da_height_too_high() {
        // given
        let prev_da_height = DaBlockHeight(100u64);
        let prev_height = 1u32.into();
        let ctx = TestContextBuilder::new()
            .with_latest_da_block_height_from_relayer(prev_da_height - 1u64.into())
            .with_prev_da_height(prev_da_height)
            .with_prev_height(prev_height)
            .build();

        let producer = ctx.producer();

        // when
        let err = producer
            .produce_and_execute_block_txpool(
                prev_height
                    .succ()
                    .expect("The block height should be valid"),
                Tai64::now(),
                (),
            )
            .await
            .expect_err("expected failure");

        // then
        assert!(
            matches!(
                err.downcast_ref::<Error>(),
                Some(Error::InvalidDaFinalizationState {
                    previous_block,
                    best
                }) if *previous_block == prev_da_height && *best == prev_da_height - 1u64.into()
            ),
            "unexpected err {err:?}"
        );
    }

    #[tokio::test]
    async fn will_only_advance_da_height_if_enough_gas_remaining() {
        // given
        let prev_da_height = 100;
        let block_gas_limit = 1_000;
        let prev_height = 1u32.into();
        // 0 + 500 + 200 + 0 + 500 = 1_200
        let latest_blocks_with_gas_costs = vec![
            (prev_da_height, 0u64),
            (prev_da_height + 1, 500),
            (prev_da_height + 2, 200),
            (prev_da_height + 3, 0),
            (prev_da_height + 4, 500),
        ]
        .into_iter()
        .map(|(height, gas_cost)| (DaBlockHeight(height), gas_cost));

        let ctx = TestContextBuilder::new()
            .with_latest_da_block_height_from_relayer((prev_da_height + 4u64).into())
            .with_latest_blocks_with_gas_costs(latest_blocks_with_gas_costs)
            .with_prev_da_height(prev_da_height.into())
            .with_block_gas_limit(block_gas_limit)
            .with_prev_height(prev_height)
            .build();

        let producer = ctx.producer();
        let next_height = prev_height
            .succ()
            .expect("The block height should be valid");

        // when
        let res = producer
            .produce_and_execute_block_txpool(next_height, Tai64::now(), ())
            .await
            .unwrap();

        // then
        let expected = prev_da_height + 3;
        let actual: u64 = res.into_result().block.header().da_height().into();
        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn will_only_advance_da_height_if_enough_transactions_remaining() {
        // given
        let prev_da_height = 100;
        let prev_height = 1u32.into();
        // 0 + 15_000 + 15_000 + 15_000 + 21_000 = 66_000 > 65_535
        let latest_blocks_with_transaction_numbers = vec![
            (prev_da_height, 0u64),
            (prev_da_height + 1, 15_000),
            (prev_da_height + 2, 15_000),
            (prev_da_height + 3, 15_000),
            (prev_da_height + 4, 21_000),
        ]
        .into_iter()
        .map(|(height, gas_cost)| (DaBlockHeight(height), gas_cost));

        let ctx = TestContextBuilder::new()
            .with_latest_da_block_height_from_relayer((prev_da_height + 4u64).into())
            .with_latest_blocks_with_transactions(latest_blocks_with_transaction_numbers)
            .with_prev_da_height(prev_da_height.into())
            .with_prev_height(prev_height)
            .build();

        let producer = ctx.producer();
        let next_height = prev_height
            .succ()
            .expect("The block height should be valid");

        // when
        let res = producer
            .produce_and_execute_block_txpool(next_height, Tai64::now(), ())
            .await
            .unwrap();

        // then
        let expected = prev_da_height + 3;
        let actual: u64 = res.into_result().block.header().da_height().into();
        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn if_each_block_is_full_then_only_advance_one_at_a_time() {
        // given
        let prev_da_height = 100;
        let block_gas_limit = 1_000;
        let prev_height = 1u32.into();
        let latest_blocks_with_gas_costs = vec![
            (prev_da_height, 1_000u64),
            (prev_da_height + 1, 1_000),
            (prev_da_height + 2, 1_000),
            (prev_da_height + 3, 1_000),
            (prev_da_height + 4, 1_000),
        ]
        .into_iter()
        .map(|(height, gas_cost)| (DaBlockHeight(height), gas_cost));

        let ctx = TestContextBuilder::new()
            .with_latest_da_block_height_from_relayer((prev_da_height + 4u64).into())
            .with_latest_blocks_with_gas_costs(latest_blocks_with_gas_costs)
            .with_prev_da_height(prev_da_height.into())
            .with_block_gas_limit(block_gas_limit)
            .with_prev_height(prev_height)
            .build();

        let producer = ctx.producer();
        let mut next_height = prev_height;

        for i in 1..=4 {
            next_height = next_height
                .succ()
                .expect("The block height should be valid");

            // when
            let res = producer
                .produce_and_execute_block_txpool(next_height, Tai64::now(), ())
                .await
                .unwrap();

            // then
            let expected = prev_da_height + i;
            let actual: u64 = res.into_result().block.header().da_height().into();
            assert_eq!(expected, actual);
        }
    }

    #[tokio::test]
    async fn if_cannot_proceed_to_next_block_throw_error() {
        use crate::block_producer::NO_NEW_DA_HEIGHT_FOUND;
        // given
        let prev_da_height = 100;
        let block_gas_limit = 1_000;
        let prev_height = 1u32.into();
        // next block cost is higher than block_gas_limit
        let latest_blocks_with_gas_costs =
            vec![(prev_da_height, 1_000u64), (prev_da_height + 1, 1_001)]
                .into_iter()
                .map(|(height, gas_cost)| (DaBlockHeight(height), gas_cost));

        let ctx = TestContextBuilder::new()
            .with_latest_da_block_height_from_relayer((prev_da_height + 1u64).into())
            .with_latest_blocks_with_gas_costs(latest_blocks_with_gas_costs)
            .with_prev_da_height(prev_da_height.into())
            .with_block_gas_limit(block_gas_limit)
            .with_prev_height(prev_height)
            .build();

        let producer = ctx.producer();
        let next_height = prev_height
            .succ()
            .expect("The block height should be valid");

        // when
        let err = producer
            .produce_and_execute_block_txpool(next_height, Tai64::now(), ())
            .await
            .unwrap_err();

        // then
        assert_eq!(&err.to_string(), NO_NEW_DA_HEIGHT_FOUND);
    }

    #[tokio::test]
    async fn production_fails_on_execution_error() {
        let ctx = TestContext::default_from_executor(FailingMockExecutor(Mutex::new(
            Some(ExecutorError::TransactionIdCollision(Default::default())),
        )));

        let producer = ctx.producer();

        let err = producer
            .produce_and_execute_block_txpool(1u32.into(), Tai64::now(), ())
            .await
            .expect_err("expected failure");

        assert!(
            matches!(
                err.downcast_ref::<ExecutorError>(),
                Some(ExecutorError::TransactionIdCollision { .. })
            ),
            "unexpected err {err:?}"
        );
    }

    // TODO: Add test that checks the gas price on the mint tx after `Executor` refactor
    //   https://github.com/FuelLabs/fuel-core/issues/1751
    #[tokio::test]
    async fn produce_and_execute_block_txpool__executor_receives_gas_price_provided() {
        // given
        let gas_price = 1_000;
        let executor = MockExecutorWithCapture::default();
        let ctx = TestContext::default_from_executor(executor.clone());

        let producer = ctx.producer_with_gas_price(Some(gas_price));

        // when
        let _ = producer
            .produce_and_execute_block_txpool(1u32.into(), Tai64::now(), ())
            .await
            .unwrap();

        // then
        let captured = executor.captured.lock().unwrap();
        let expected = gas_price;
        let actual = captured
            .as_ref()
            .expect("expected executor to be called")
            .gas_price;
        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn produce_and_execute_block_txpool__missing_gas_price_causes_block_production_to_fail()
     {
        // given
        let ctx = TestContext::default();
        let producer = ctx.producer_with_gas_price(None);

        // when
        let result = producer
            .produce_and_execute_block_txpool(1u32.into(), Tai64::now(), ())
            .await;

        // then
        assert!(result.is_err());
    }
}

// Tests for the `dry_run` method.
mod dry_run {
    use super::*;

    #[tokio::test]
    async fn dry_run__executes_with_given_timestamp() {
        // Given
        let simulated_block_time = Tai64::from_unix(1337);
        let executor = MockExecutorWithCapture::default();
        let ctx = TestContext::default_from_executor(executor.clone());

        // When
        let _ = ctx
            .producer()
            .dry_run(vec![], None, Some(simulated_block_time), None, None, false)
            .await;

        // Then
        assert_eq!(executor.captured_block_timestamp(), simulated_block_time);
    }

    #[tokio::test]
    async fn dry_run__executes_with_past_timestamp() {
        // Given
        let simulated_block_time = Tai64::UNIX_EPOCH;
        let last_block_time = Tai64::from_unix(1337);

        let executor = MockExecutorWithCapture::default();
        let ctx = TestContextBuilder::new()
            .with_prev_time(last_block_time)
            .build_with_executor(executor.clone());

        // When
        let _ = ctx
            .producer()
            .dry_run(vec![], None, Some(simulated_block_time), None, None, false)
            .await;

        // Then
        assert_eq!(executor.captured_block_timestamp(), simulated_block_time);
    }

    #[tokio::test]
    async fn dry_run__uses_last_block_timestamp_when_no_time_provided() {
        // Given
        let last_block_time = Tai64::from_unix(1337);

        let executor = MockExecutorWithCapture::default();
        let ctx = TestContextBuilder::new()
            .with_prev_time(last_block_time)
            .build_with_executor(executor.clone());

        // When
        let _ = ctx
            .producer()
            .dry_run(vec![], None, None, None, None, false)
            .await;

        // Then
        assert_eq!(executor.captured_block_timestamp(), last_block_time);
    }

    #[tokio::test]
    async fn dry_run__success_when_height_is_the_same_as_chain_height() {
        let executor = MockExecutorWithCapture::default();
        let ctx = TestContext::default_from_executor(executor);
        let producer = ctx.producer();

        const SAME_HEIGHT: u32 = 1;

        // Given
        let block = producer
            .produce_and_execute_block_txpool(SAME_HEIGHT.into(), Tai64::now(), ())
            .await
            .unwrap();
        producer.view_provider.blocks.lock().unwrap().insert(
            SAME_HEIGHT.into(),
            block.result().block.clone().compress(&Default::default()),
        );

        // When
        let result = producer
            .dry_run(vec![], Some(SAME_HEIGHT.into()), None, None, None, false)
            .await;

        // Then
        assert!(result.is_ok(), "{:?}", result);
    }

    impl MockExecutorWithCapture {
        fn captured_block_timestamp(&self) -> Tai64 {
            *self
                .captured
                .lock()
                .unwrap()
                .as_ref()
                .expect("should have captured a block")
                .header_to_produce
                .time()
        }
    }
}

use fuel_core_types::fuel_tx::field::MintGasPrice;
use proptest::{
    prop_compose,
    proptest,
};

prop_compose! {
    fn arb_block()(height in 1..255u8, da_height in 1..255u64, gas_price: u64, coinbase_recipient: [u8; 32], num_txs in 0..100u32) -> Block {
        let mut txs : Vec<_> = (0..num_txs).map(|_| Transaction::Script(Script::default())).collect();
        let mut inner_mint = Mint::default();
        *inner_mint.gas_price_mut() = gas_price;
        *inner_mint.input_contract_mut() = fuel_tx::input::contract::Contract{
            contract_id: coinbase_recipient.into(),
            ..Default::default()
        };

        let mint = Transaction::Mint(inner_mint);
        txs.push(mint);
        let header = PartialBlockHeader {
            consensus: ConsensusHeader {
                height: (height as u32).into(),
                ..Default::default()
            },
            application: ApplicationHeader {
                da_height: DaBlockHeight(da_height),
                ..Default::default()
            },
        };
        let outbox_message_ids = vec![];
        let event_inbox_root = Bytes32::default();
        Block::new(
            header,
            txs,
            &outbox_message_ids,
            event_inbox_root,
            #[cfg(feature = "fault-proving")] &Default::default(),
        ).unwrap()
    }
}

#[allow(clippy::arithmetic_side_effects)]
fn ctx_for_block(
    block: &Block,
    executor: MockExecutorWithCapture,
) -> TestContext<MockExecutorWithCapture> {
    let prev_height = block.header().height().pred().unwrap();
    let prev_da_height = block.header().da_height().as_u64() - 1;
    TestContextBuilder::new()
        .with_prev_height(prev_height)
        .with_prev_da_height(prev_da_height.into())
        .build_with_executor(executor)
}

// gas_price
proptest! {
    #[test]
    fn produce_and_execute_predefined_block__contains_expected_gas_price(block in arb_block()) {
        let rt = multithreaded_runtime();

        // given
        let executor = MockExecutorWithCapture::default();
        let ctx = ctx_for_block(&block, executor.clone());

        //when
        let _ =  rt.block_on(ctx.producer().produce_and_execute_predefined(&block, ())).unwrap();

        // then
        let expected_gas_price = *block
            .transactions().last().and_then(|tx| tx.as_mint()).unwrap().gas_price();
        let captured = executor.captured.lock().unwrap();
        let actual = captured.as_ref().unwrap().gas_price;
        assert_eq!(expected_gas_price, actual);
    }

    // time
    #[test]
    fn produce_and_execute_predefined_block__contains_expected_time(block in arb_block()) {
        let rt = multithreaded_runtime();

        // given
        let executor = MockExecutorWithCapture::default();
        let ctx = ctx_for_block(&block, executor.clone());

        //when
        let _ =  rt.block_on(ctx.producer().produce_and_execute_predefined(&block, ())).unwrap();

        // then
        let expected_time = block.header().consensus().time;
        let captured = executor.captured.lock().unwrap();
        let actual = captured.as_ref().unwrap().header_to_produce.consensus.time;
        assert_eq!(expected_time, actual);
    }

    // coinbase
    #[test]
    fn produce_and_execute_predefined_block__contains_expected_coinbase_recipient(block in arb_block()) {
        let rt = multithreaded_runtime();

        // given
        let executor = MockExecutorWithCapture::default();
        let ctx = ctx_for_block(&block, executor.clone());

        //when
        let _ =  rt.block_on(ctx.producer().produce_and_execute_predefined(&block, ())).unwrap();

        // then
        let expected_coinbase = block.transactions().last().and_then(|tx| tx.as_mint()).unwrap().input_contract().contract_id;
        let captured = executor.captured.lock().unwrap();
        let actual = captured.as_ref().unwrap().coinbase_recipient;
        assert_eq!(expected_coinbase, actual);
    }

    // DA height
    #[test]
    fn produce_and_execute_predefined_block__contains_expected_da_height(block in arb_block()) {
        let rt = multithreaded_runtime();

        // given
        let executor = MockExecutorWithCapture::default();
        let ctx = ctx_for_block(&block, executor.clone());

        //when
        let _ =  rt.block_on(ctx.producer().produce_and_execute_predefined(&block, ())).unwrap();

        // then
        let expected_da_height = block.header().da_height();
        let captured = executor.captured.lock().unwrap();
        let actual = captured.as_ref().unwrap().header_to_produce.application.da_height;
        assert_eq!(expected_da_height, actual);
    }

    #[test]
    fn produce_and_execute_predefined_block__do_not_include_original_mint_in_txs_source(block in arb_block()) {
        let rt = multithreaded_runtime();

        // given
        let executor = MockExecutorWithCapture::default();
        let ctx = ctx_for_block(&block, executor.clone());

        //when
        let _ =  rt.block_on(ctx.producer().produce_and_execute_predefined(&block, ())).unwrap();

        // then
        let captured = executor.captured.lock().unwrap();
        let txs_source = &captured.as_ref().unwrap().transactions_source;
        let has_a_mint = txs_source.iter().any(|tx| matches!(tx, Transaction::Mint(_)));
        assert!(!has_a_mint);
    }
}

fn multithreaded_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

struct TestContext<Executor> {
    config: Config,
    db: MockDb,
    relayer: MockRelayer,
    executor: Arc<Executor>,
    txpool: MockTxPool,
    gas_price: Option<u64>,
    block_gas_limit: u64,
}

impl TestContext<MockExecutor> {
    pub fn default() -> Self {
        Self::default_from_db(Self::default_db())
    }

    pub fn default_from_db(db: MockDb) -> Self {
        let executor = MockExecutor(db.clone());
        Self::default_from_db_and_executor(db, executor)
    }
}

impl<Executor> TestContext<Executor> {
    fn default_db() -> MockDb {
        let genesis_height = 0u32.into();
        let genesis_block = CompressedBlock::default();

        MockDb {
            blocks: Arc::new(Mutex::new(
                vec![(genesis_height, genesis_block)].into_iter().collect(),
            )),
            consensus_parameters_version: 0,
            state_transition_bytecode_version: 0,
        }
    }

    pub fn default_from_executor(executor: Executor) -> Self {
        Self::default_from_db_and_executor(Self::default_db(), executor)
    }

    pub fn default_from_db_and_executor(db: MockDb, executor: Executor) -> Self {
        let txpool = MockTxPool::default();
        let relayer = MockRelayer::default();
        let config = Config::default();
        let gas_price = Some(0);
        Self {
            config,
            db,
            relayer,
            executor: Arc::new(executor),
            txpool,
            gas_price,
            block_gas_limit: 0,
        }
    }

    pub fn producer(
        self,
    ) -> Producer<
        MockDb,
        MockTxPool,
        Executor,
        MockProducerGasPrice,
        MockChainStateInfoProvider,
    > {
        let gas_price = self.gas_price;
        let static_gas_price = MockProducerGasPrice::new(gas_price);

        let mut consensus_params = ConsensusParameters::default();
        consensus_params.set_block_gas_limit(self.block_gas_limit);
        let consensus_params = Arc::new(consensus_params);

        let mut chain_state_info_provider = MockChainStateInfoProvider::default();
        chain_state_info_provider
            .expect_consensus_params_at_version()
            .returning(move |_| Ok(consensus_params.clone()));

        Producer {
            config: self.config,
            view_provider: self.db,
            txpool: self.txpool,
            executor: self.executor,
            relayer: Box::new(self.relayer),
            lock: Default::default(),
            gas_price_provider: static_gas_price,
            chain_state_info_provider,
        }
    }

    pub fn producer_with_gas_price(
        mut self,
        gas_price: Option<u64>,
    ) -> Producer<
        MockDb,
        MockTxPool,
        Executor,
        MockProducerGasPrice,
        MockChainStateInfoProvider,
    > {
        self.gas_price = gas_price;
        self.producer()
    }
}

struct TestContextBuilder {
    latest_block_height: DaBlockHeight,
    blocks_with_gas_costs_and_transactions_number: HashMap<DaBlockHeight, (u64, u64)>,
    prev_da_height: DaBlockHeight,
    block_gas_limit: Option<u64>,
    prev_height: BlockHeight,
    prev_time: Tai64,
}

impl TestContextBuilder {
    fn new() -> Self {
        Self {
            latest_block_height: 0u64.into(),
            blocks_with_gas_costs_and_transactions_number: HashMap::new(),
            prev_da_height: 1u64.into(),
            block_gas_limit: None,
            prev_height: 0u32.into(),
            prev_time: Tai64::UNIX_EPOCH,
        }
    }

    fn with_latest_da_block_height_from_relayer(
        mut self,
        latest_block_height: DaBlockHeight,
    ) -> Self {
        self.latest_block_height = latest_block_height;
        self
    }

    fn with_latest_blocks_with_gas_costs_and_transactions_number(
        mut self,
        latest_blocks_with_gas_costs_and_transactions: impl Iterator<
            Item = (DaBlockHeight, (u64, u64)),
        >,
    ) -> Self {
        self.blocks_with_gas_costs_and_transactions_number
            .extend(latest_blocks_with_gas_costs_and_transactions);
        self
    }

    // Helper function that can be used in tests where transaction numbers in a da block are irrelevant
    fn with_latest_blocks_with_gas_costs(
        mut self,
        latest_blocks_with_gas_costs: impl Iterator<Item = (DaBlockHeight, u64)>,
    ) -> Self {
        let latest_blocks_with_gas_costs_and_transactions_number =
            latest_blocks_with_gas_costs
                .into_iter()
                .map(|(da_block_height, gas_costs)| (da_block_height, (gas_costs, 0)));
        // Assigning `self` necessary to avoid the compiler complaining about the mutability of `self`
        self = self.with_latest_blocks_with_gas_costs_and_transactions_number(
            latest_blocks_with_gas_costs_and_transactions_number,
        );
        self
    }

    // Helper function that can be used in tests where gas costs in a da block are irrelevant
    fn with_latest_blocks_with_transactions(
        mut self,
        latest_blocks_with_transactions: impl Iterator<Item = (DaBlockHeight, u64)>,
    ) -> Self {
        let latest_blocks_with_gas_costs_and_transactions_number =
            latest_blocks_with_transactions.into_iter().map(
                |(da_block_height, transactions)| (da_block_height, (0, transactions)),
            );

        // Assigning `self` necessary to avoid the compiler complaining about the mutability of `self`
        self = self.with_latest_blocks_with_gas_costs_and_transactions_number(
            latest_blocks_with_gas_costs_and_transactions_number,
        );
        self
    }

    fn with_prev_da_height(mut self, prev_da_height: DaBlockHeight) -> Self {
        self.prev_da_height = prev_da_height;
        self
    }

    fn with_block_gas_limit(mut self, block_gas_limit: u64) -> Self {
        self.block_gas_limit = Some(block_gas_limit);
        self
    }

    fn with_prev_height(mut self, prev_height: BlockHeight) -> Self {
        self.prev_height = prev_height;
        self
    }

    fn with_prev_time(mut self, prev_time: Tai64) -> Self {
        self.prev_time = prev_time;
        self
    }

    fn pre_existing_blocks(&self) -> Arc<Mutex<HashMap<BlockHeight, CompressedBlock>>> {
        let da_height = self.prev_da_height;
        let height = self.prev_height;
        let time = self.prev_time;

        let block = PartialFuelBlock {
            header: PartialBlockHeader {
                application: ApplicationHeader {
                    da_height,
                    ..Default::default()
                },
                consensus: ConsensusHeader {
                    height,
                    time,
                    ..Default::default()
                },
            },
            transactions: vec![],
        }
        .generate(
            &[],
            Default::default(),
            #[cfg(feature = "fault-proving")]
            &Default::default(),
        )
        .unwrap()
        .compress(&Default::default());

        Arc::new(Mutex::new(HashMap::from_iter(Some((height, block)))))
    }

    fn build(self) -> TestContext<MockExecutor> {
        let block_gas_limit = self.block_gas_limit.unwrap_or_default();

        let mock_relayer = MockRelayer {
            latest_block_height: self.latest_block_height,
            latest_da_blocks_with_costs_and_transactions_number: self
                .blocks_with_gas_costs_and_transactions_number
                .clone(),
            ..Default::default()
        };

        let db = MockDb {
            blocks: self.pre_existing_blocks(),
            consensus_parameters_version: 0,
            state_transition_bytecode_version: 0,
        };

        TestContext {
            relayer: mock_relayer,
            block_gas_limit,
            ..TestContext::default_from_db(db)
        }
    }

    fn build_with_executor<Ex>(self, executor: Ex) -> TestContext<Ex> {
        let block_gas_limit = self.block_gas_limit.unwrap_or_default();

        let mock_relayer = MockRelayer {
            latest_block_height: self.latest_block_height,
            latest_da_blocks_with_costs_and_transactions_number: self
                .blocks_with_gas_costs_and_transactions_number
                .clone(),
            ..Default::default()
        };

        let db = MockDb {
            blocks: self.pre_existing_blocks(),
            consensus_parameters_version: 0,
            state_transition_bytecode_version: 0,
        };

        TestContext {
            relayer: mock_relayer,
            block_gas_limit,
            ..TestContext::default_from_db_and_executor(db, executor)
        }
    }
}
