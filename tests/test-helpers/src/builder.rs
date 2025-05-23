use fuel_core::{
    chain_config::{
        ChainConfig,
        CoinConfig,
        ContractBalanceConfig,
        ContractConfig,
        LastBlockConfig,
        SnapshotMetadata,
        StateConfig,
    },
    service::{
        Config,
        DbType,
        FuelService,
        config::GasPriceConfig,
    },
    state::rocks_db::DatabaseConfig,
};
use fuel_core_client::client::FuelClient;
use fuel_core_poa::Trigger;
use fuel_core_types::{
    blockchain::header::LATEST_STATE_TRANSITION_VERSION,
    fuel_asm::op,
    fuel_tx::{
        field::Inputs,
        input::coin::{
            CoinPredicate,
            CoinSigned,
        },
        policies::Policies,
        *,
    },
    fuel_types::BlockHeight,
};
use itertools::Itertools;
use rand::{
    Rng,
    SeedableRng,
    rngs::StdRng,
};
use std::{
    collections::HashMap,
    io,
};

/// Helper for wrapping a currently running node environment
pub struct TestContext {
    pub srv: FuelService,
    pub rng: StdRng,
    pub client: FuelClient,
}

impl TestContext {
    pub async fn new(seed: u64) -> Self {
        let rng = StdRng::seed_from_u64(seed);
        let srv = FuelService::new_node(Config::local_node()).await.unwrap();
        let client = FuelClient::from(srv.bound_address);
        Self { srv, rng, client }
    }

    pub async fn transfer(
        &mut self,
        from: Address,
        to: Address,
        amount: u64,
    ) -> io::Result<Bytes32> {
        let script = op::ret(0x10).to_bytes().to_vec();
        let tx = Transaction::script(
            1_000_000,
            script,
            vec![],
            Policies::new().with_max_fee(0),
            vec![Input::coin_signed(
                self.rng.r#gen(),
                from,
                amount,
                Default::default(),
                Default::default(),
                Default::default(),
            )],
            vec![Output::coin(to, amount, Default::default())],
            vec![vec![].into()],
        )
        .into();
        self.client.submit_and_await_commit(&tx).await?;
        Ok(tx.id(&Default::default()))
    }
}

/// Helper for configuring the genesis block in tests
#[derive(Clone)]
pub struct TestSetupBuilder {
    pub rng: StdRng,
    pub contracts: HashMap<ContractId, ContractConfig>,
    pub initial_coins: Vec<CoinConfig>,
    pub starting_gas_price: u64,
    pub gas_limit: Option<u64>,
    pub block_size_limit: Option<u64>,
    pub starting_block: Option<BlockHeight>,
    pub utxo_validation: bool,
    pub privileged_address: Address,
    pub base_asset_id: AssetId,
    pub trigger: Trigger,
    pub max_txs: usize,
    pub database_type: DbType,
    pub database_config: DatabaseConfig,
    pub chain_config: Option<ChainConfig>,
    pub number_threads_pool_verif: usize,
}

impl TestSetupBuilder {
    pub fn new(seed: u64) -> TestSetupBuilder {
        Self {
            rng: StdRng::seed_from_u64(seed),
            number_threads_pool_verif: 0,
            ..Default::default()
        }
    }

    pub fn set_chain_config(&mut self, chain_config: ChainConfig) -> &mut Self {
        self.chain_config = Some(chain_config);
        self
    }

    /// setup a contract and add to genesis configuration
    pub fn setup_contract(
        &mut self,
        code: Vec<u8>,
        balances: Vec<ContractBalanceConfig>,
        tx_pointer: Option<TxPointer>,
    ) -> (Salt, ContractId) {
        let contract = Contract::from(code.clone());
        let root = contract.root();
        let salt: Salt = self.rng.r#gen();
        let contract_id = contract.id(&salt, &root, &Contract::default_state_root());

        let tx_pointer = tx_pointer.unwrap_or_default();
        let utxo_id: UtxoId = self.rng.r#gen();
        self.contracts.insert(
            contract_id,
            ContractConfig {
                contract_id,
                code,
                tx_id: *utxo_id.tx_id(),
                output_index: utxo_id.output_index(),
                tx_pointer_block_height: tx_pointer.block_height(),
                tx_pointer_tx_idx: tx_pointer.tx_index(),
                states: vec![],
                balances,
            },
        );

        (salt, contract_id)
    }

    /// add input coins from a set of transaction to the genesis config
    pub fn config_coin_inputs_from_transactions<T>(
        &mut self,
        transactions: &[&T],
    ) -> &mut Self
    where
        T: Inputs,
    {
        self.initial_coins.extend(
            transactions
                .iter()
                .flat_map(|t| t.inputs())
                .filter_map(|input| {
                    if let Input::CoinSigned(CoinSigned {
                        amount,
                        owner,
                        asset_id,
                        utxo_id,
                        tx_pointer,
                        ..
                    })
                    | Input::CoinPredicate(CoinPredicate {
                        amount,
                        owner,
                        asset_id,
                        utxo_id,
                        tx_pointer,
                        ..
                    }) = input
                    {
                        Some(CoinConfig {
                            tx_id: *utxo_id.tx_id(),
                            output_index: utxo_id.output_index(),
                            tx_pointer_block_height: tx_pointer.block_height(),
                            tx_pointer_tx_idx: tx_pointer.tx_index(),
                            owner: *owner,
                            amount: *amount,
                            asset_id: *asset_id,
                        })
                    } else {
                        None
                    }
                }),
        );

        self
    }

    // setup chainspec and spin up a fuel-node
    pub async fn finalize(&mut self) -> TestContext {
        let mut chain_conf = self.chain_config.clone().unwrap_or_else(local_chain_config);

        if let Some(gas_limit) = self.gas_limit {
            let tx_params = *chain_conf.consensus_parameters.tx_params();
            chain_conf
                .consensus_parameters
                .set_tx_params(tx_params.with_max_gas_per_tx(gas_limit));
            chain_conf
                .consensus_parameters
                .set_block_gas_limit(gas_limit);
        }

        if let Some(block_size_limit) = self.block_size_limit {
            chain_conf
                .consensus_parameters
                .set_block_transaction_size_limit(block_size_limit)
                .expect("Should set new block size limit");
        }

        chain_conf
            .consensus_parameters
            .set_privileged_address(self.privileged_address);
        chain_conf
            .consensus_parameters
            .set_base_asset_id(self.base_asset_id);
        chain_conf.state_transition_bytecode =
            fuel_core::upgradable_executor::WASM_BYTECODE.to_vec();

        let latest_block = self.starting_block.map(|starting_block| LastBlockConfig {
            block_height: starting_block,
            state_transition_version: LATEST_STATE_TRANSITION_VERSION - 1,
            ..Default::default()
        });

        let state = StateConfig {
            coins: self.initial_coins.clone(),
            contracts: self.contracts.values().cloned().collect_vec(),
            last_block: latest_block,
            ..StateConfig::default()
        };

        let mut txpool = fuel_core_txpool::config::Config::default();
        txpool.pool_limits.max_txs = self.max_txs;
        txpool.service_channel_limits = fuel_core_txpool::config::ServiceChannelLimits {
            max_pending_write_pool_requests: self.max_txs,
            max_pending_read_pool_requests: self.max_txs,
        };
        txpool.heavy_work.size_of_verification_queue = self.max_txs;
        txpool.heavy_work.number_threads_to_verify_transactions =
            self.number_threads_pool_verif;
        txpool.utxo_validation = self.utxo_validation;

        let gas_price_config = GasPriceConfig {
            starting_exec_gas_price: self.starting_gas_price,
            ..GasPriceConfig::local_node()
        };

        let mut config = Config {
            utxo_validation: self.utxo_validation,
            txpool,
            block_production: self.trigger,
            gas_price_config,
            ..Config::local_node_with_configs(chain_conf, state)
        };
        config.combined_db_config.database_config = self.database_config;

        let srv = FuelService::new_node(config).await.unwrap();
        let client = FuelClient::from(srv.bound_address);

        TestContext {
            srv,
            rng: self.rng.clone(),
            client,
        }
    }
}

impl Default for TestSetupBuilder {
    fn default() -> Self {
        TestSetupBuilder {
            rng: StdRng::seed_from_u64(2322u64),
            contracts: Default::default(),
            initial_coins: vec![],
            starting_gas_price: 0,
            gas_limit: None,
            block_size_limit: None,
            starting_block: None,
            utxo_validation: true,
            privileged_address: Default::default(),
            base_asset_id: AssetId::BASE,
            trigger: Trigger::Instant,
            max_txs: 100000,
            database_type: DbType::RocksDb,
            database_config: DatabaseConfig::config_for_tests(),
            chain_config: None,
            number_threads_pool_verif: 0,
        }
    }
}

pub fn local_chain_config() -> ChainConfig {
    let metadata =
        SnapshotMetadata::read("../bin/fuel-core/chainspec/local-testnet").unwrap();
    ChainConfig::from_snapshot_metadata(&metadata).unwrap()
}
