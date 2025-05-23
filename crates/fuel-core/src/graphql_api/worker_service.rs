use self::indexation::error::IndexationError;

use super::{
    block_height_subscription,
    indexation,
    storage::old::{
        OldFuelBlockConsensus,
        OldFuelBlocks,
        OldTransactions,
    },
};
use crate::{
    fuel_core_graphql_api::{
        ports::{
            self,
            worker::{
                BlockAt,
                OffChainDatabaseTransaction,
            },
        },
        storage::{
            blocks::FuelBlockIdsToHeights,
            coins::{
                OwnedCoins,
                owner_coin_id_key,
            },
            contracts::ContractsInfo,
            messages::{
                OwnedMessageIds,
                OwnedMessageKey,
                SpentMessages,
            },
        },
    },
    graphql_api::{
        query_costs,
        storage::relayed_transactions::RelayedTransactionStatuses,
    },
};
use fuel_core_metrics::graphql_metrics::graphql_metrics;
use fuel_core_services::{
    RunnableService,
    RunnableTask,
    ServiceRunner,
    StateWatcher,
    TaskNextAction,
    stream::BoxStream,
};
use fuel_core_storage::{
    Error as StorageError,
    Result as StorageResult,
    StorageAsMut,
};
use fuel_core_tx_status_manager::from_executor_to_status;
use fuel_core_types::{
    blockchain::{
        block::{
            Block,
            CompressedBlock,
        },
        consensus::Consensus,
    },
    entities::relayer::transaction::RelayedTransactionStatus,
    fuel_tx::{
        AssetId,
        ConsensusParameters,
        Contract,
        Input,
        Output,
        Receipt,
        Transaction,
        TxId,
        UniqueIdentifier,
        field::{
            Inputs,
            Outputs,
            Salt,
            StorageSlots,
        },
        input::coin::{
            CoinPredicate,
            CoinSigned,
        },
    },
    fuel_types::{
        BlockHeight,
        Bytes32,
        ChainId,
    },
    services::{
        block_importer::{
            ImportResult,
            SharedImportResult,
        },
        executor::{
            Event,
            TransactionExecutionResult,
            TransactionExecutionStatus,
        },
    },
};
use futures::{
    FutureExt,
    StreamExt,
};
use std::{
    borrow::Cow,
    ops::Deref,
};
#[cfg(test)]
mod tests;

pub(crate) struct Context<'a, TxStatusManager, BlockImporter, OnChain, OffChain> {
    pub(crate) tx_status_manager: TxStatusManager,
    pub(crate) block_importer: BlockImporter,
    pub(crate) on_chain_database: OnChain,
    pub(crate) off_chain_database: OffChain,
    pub(crate) continue_on_error: bool,
    pub(crate) consensus_parameters: &'a ConsensusParameters,
}

/// The initialization task recovers the state of the GraphQL service database on startup.
pub struct InitializeTask<TxStatusManager, BlockImporter, OnChain, OffChain> {
    chain_id: ChainId,
    continue_on_error: bool,
    tx_status_manager: TxStatusManager,
    blocks_events: BoxStream<SharedImportResult>,
    block_importer: BlockImporter,
    on_chain_database: OnChain,
    off_chain_database: OffChain,
    base_asset_id: AssetId,
    block_height_subscription_handler: block_height_subscription::Handler,
}

/// The off-chain GraphQL API worker task processes the imported blocks
/// and actualize the information used by the GraphQL service.
pub struct Task<TxStatusManager, D> {
    tx_status_manager: TxStatusManager,
    block_importer: BoxStream<SharedImportResult>,
    database: D,
    chain_id: ChainId,
    continue_on_error: bool,
    balances_indexation_enabled: bool,
    coins_to_spend_indexation_enabled: bool,
    asset_metadata_indexation_enabled: bool,
    base_asset_id: AssetId,
    block_height_subscription_handler: block_height_subscription::Handler,
}

impl<TxStatusManager, D> Task<TxStatusManager, D>
where
    TxStatusManager: ports::worker::TxStatusCompletion,
    D: ports::worker::OffChainDatabase,
{
    fn process_block(&mut self, result: SharedImportResult) -> anyhow::Result<()> {
        let block = &result.sealed_block.entity;
        let mut transaction = self.database.transaction();
        // save the status for every transaction using the finalized block id
        persist_transaction_status(
            &result,
            self.asset_metadata_indexation_enabled,
            &mut transaction,
        )?;

        // save the associated owner for each transaction in the block
        index_tx_owners_for_block(block, &mut transaction, &self.chain_id)?;

        // save the transaction related information
        process_transactions(block.transactions().iter(), &mut transaction)?;

        let height = block.header().height();
        let block_id = block.id();
        transaction
            .storage_as_mut::<FuelBlockIdsToHeights>()
            .insert(&block_id, height)?;

        let total_tx_count = transaction
            .increase_tx_count(block.transactions().len() as u64)
            .unwrap_or_default();

        process_executor_events(
            result.events.iter().map(Cow::Borrowed),
            &mut transaction,
            self.balances_indexation_enabled,
            self.coins_to_spend_indexation_enabled,
            &self.base_asset_id,
        )?;

        transaction.commit()?;

        for status in result.tx_status.iter() {
            let tx_id = status.id;
            let status = from_executor_to_status(block, status.result.clone());

            self.tx_status_manager
                .send_complete(tx_id, height, status.into());
        }

        // Notify subscribers and update last seen block height
        self.block_height_subscription_handler
            .notify_and_update(*height);
        // Get all the subscribers that need to be notified that the block height
        // has been reached.

        // update the importer metrics after the block is successfully committed
        graphql_metrics().total_txs_count.set(total_tx_count as i64);

        Ok(())
    }
}

/// Process the executor events and update the indexes for the messages and coins.
pub fn process_executor_events<'a, Iter, T>(
    events: Iter,
    block_st_transaction: &mut T,
    balances_indexation_enabled: bool,
    coins_to_spend_indexation_enabled: bool,
    base_asset_id: &AssetId,
) -> anyhow::Result<()>
where
    Iter: Iterator<Item = Cow<'a, Event>>,
    T: OffChainDatabaseTransaction,
{
    for event in events {
        match update_event_based_indexation(
            &event,
            block_st_transaction,
            balances_indexation_enabled,
            coins_to_spend_indexation_enabled,
            base_asset_id,
        ) {
            Ok(()) => (),
            Err(IndexationError::StorageError(err)) => {
                return Err(err.into());
            }
            Err(err) => {
                // TODO[RC]: Indexation errors to be correctly handled. See: https://github.com/FuelLabs/fuel-core/issues/2428
                tracing::error!("Indexation error: {}", err);
            }
        };
        match event.deref() {
            Event::MessageImported(message) => {
                block_st_transaction
                    .storage_as_mut::<OwnedMessageIds>()
                    .insert(
                        &OwnedMessageKey::new(message.recipient(), message.nonce()),
                        &(),
                    )?;
            }
            Event::MessageConsumed(message) => {
                block_st_transaction
                    .storage_as_mut::<OwnedMessageIds>()
                    .remove(&OwnedMessageKey::new(
                        message.recipient(),
                        message.nonce(),
                    ))?;
                block_st_transaction
                    .storage::<SpentMessages>()
                    .insert(message.nonce(), &())?;
            }
            Event::CoinCreated(coin) => {
                let coin_by_owner = owner_coin_id_key(&coin.owner, &coin.utxo_id);
                block_st_transaction
                    .storage_as_mut::<OwnedCoins>()
                    .insert(&coin_by_owner, &())?;
            }
            Event::CoinConsumed(coin) => {
                let key = owner_coin_id_key(&coin.owner, &coin.utxo_id);
                block_st_transaction
                    .storage_as_mut::<OwnedCoins>()
                    .remove(&key)?;
            }
            Event::ForcedTransactionFailed {
                id,
                block_height,
                failure,
            } => {
                let status = RelayedTransactionStatus::Failed {
                    block_height: *block_height,
                    failure: failure.clone(),
                };

                block_st_transaction
                    .storage_as_mut::<RelayedTransactionStatuses>()
                    .insert(&Bytes32::from(id.to_owned()), &status)?;
            }
        }
    }
    Ok(())
}

fn update_event_based_indexation<T>(
    event: &Event,
    block_st_transaction: &mut T,
    balances_indexation_enabled: bool,
    coins_to_spend_indexation_enabled: bool,
    base_asset_id: &AssetId,
) -> Result<(), IndexationError>
where
    T: OffChainDatabaseTransaction,
{
    indexation::balances::update(
        event,
        block_st_transaction,
        balances_indexation_enabled,
    )?;

    indexation::coins_to_spend::update(
        event,
        block_st_transaction,
        coins_to_spend_indexation_enabled,
        base_asset_id,
    )?;

    Ok(())
}

fn update_receipt_based_indexation<T>(
    receipts: &[Receipt],
    block_st_transaction: &mut T,
    asset_metadata_indexation_enabled: bool,
) -> Result<(), IndexationError>
where
    T: OffChainDatabaseTransaction,
{
    indexation::asset_metadata::update(
        receipts,
        block_st_transaction,
        asset_metadata_indexation_enabled,
    )?;

    Ok(())
}

/// Associate all transactions within a block to their respective UTXO owners
fn index_tx_owners_for_block<T>(
    block: &Block,
    block_st_transaction: &mut T,
    chain_id: &ChainId,
) -> anyhow::Result<()>
where
    T: OffChainDatabaseTransaction,
{
    for (tx_idx, tx) in block.transactions().iter().enumerate() {
        let block_height = *block.header().height();
        let inputs;
        let outputs;
        let tx_idx = u16::try_from(tx_idx).map_err(|e| {
            anyhow::anyhow!("The block has more than `u16::MAX` transactions, {}", e)
        })?;
        let tx_id = tx.id(chain_id);
        match tx {
            Transaction::Script(tx) => {
                inputs = tx.inputs().as_slice();
                outputs = tx.outputs().as_slice();
            }
            Transaction::Create(tx) => {
                inputs = tx.inputs().as_slice();
                outputs = tx.outputs().as_slice();
            }
            Transaction::Mint(_) => continue,
            Transaction::Upgrade(tx) => {
                inputs = tx.inputs().as_slice();
                outputs = tx.outputs().as_slice();
            }
            Transaction::Upload(tx) => {
                inputs = tx.inputs().as_slice();
                outputs = tx.outputs().as_slice();
            }
            Transaction::Blob(tx) => {
                inputs = tx.inputs().as_slice();
                outputs = tx.outputs().as_slice();
            }
        }
        persist_owners_index(
            block_height,
            inputs,
            outputs,
            &tx_id,
            tx_idx,
            block_st_transaction,
        )?;
    }
    Ok(())
}

/// Index the tx id by owner for all of the inputs and outputs
fn persist_owners_index<T>(
    block_height: BlockHeight,
    inputs: &[Input],
    outputs: &[Output],
    tx_id: &Bytes32,
    tx_idx: u16,
    db: &mut T,
) -> StorageResult<()>
where
    T: OffChainDatabaseTransaction,
{
    let mut owners = vec![];
    for input in inputs {
        if let Input::CoinSigned(CoinSigned { owner, .. })
        | Input::CoinPredicate(CoinPredicate { owner, .. }) = input
        {
            owners.push(owner);
        }
    }

    for output in outputs {
        match output {
            Output::Coin { to, .. }
            | Output::Change { to, .. }
            | Output::Variable { to, .. } => {
                owners.push(to);
            }
            Output::Contract(_) | Output::ContractCreated { .. } => {}
        }
    }

    // dedupe owners from inputs and outputs prior to indexing
    owners.sort();
    owners.dedup();

    for owner in owners {
        db.record_tx_id_owner(owner, block_height, tx_idx, tx_id)?;
    }

    Ok(())
}

fn persist_transaction_status<T>(
    import_result: &ImportResult,
    asset_metadata_indexation_enabled: bool,
    db: &mut T,
) -> StorageResult<()>
where
    T: OffChainDatabaseTransaction,
{
    for TransactionExecutionStatus { id, result } in import_result.tx_status.iter() {
        let status =
            from_executor_to_status(&import_result.sealed_block.entity, result.clone());

        if db.update_tx_status(id, status)?.is_some() {
            return Err(anyhow::anyhow!(
                "Transaction status already exists for tx {}",
                id
            )
            .into());
        }

        let TransactionExecutionResult::Success { receipts, .. } = result else {
            continue
        };

        update_receipt_based_indexation(receipts, db, asset_metadata_indexation_enabled)?;
    }
    Ok(())
}

pub fn process_transactions<'a, I, T>(transactions: I, db: &mut T) -> StorageResult<()>
where
    I: Iterator<Item = &'a Transaction>,
    T: OffChainDatabaseTransaction,
{
    for tx in transactions {
        match tx {
            Transaction::Create(tx) => {
                let contract_id = tx
                    .outputs()
                    .iter()
                    .filter_map(|output| output.contract_id().cloned())
                    .next()
                    .map(Ok::<_, StorageError>)
                    .unwrap_or_else(|| {
                        // TODO: Reuse `CreateMetadata` when it will be exported
                        //  from the `fuel-tx` crate.
                        let salt = tx.salt();
                        let storage_slots = tx.storage_slots();
                        let contract = Contract::try_from(tx)
                            .map_err(|e| anyhow::anyhow!("{:?}", e))?;
                        let contract_root = contract.root();
                        let state_root =
                            Contract::initial_state_root(storage_slots.iter());
                        Ok::<_, StorageError>(contract.id(
                            salt,
                            &contract_root,
                            &state_root,
                        ))
                    })?;

                let salt = *tx.salt();

                db.storage::<ContractsInfo>()
                    .insert(&contract_id, &(salt.into()))?;
            }
            Transaction::Script(_)
            | Transaction::Mint(_)
            | Transaction::Upgrade(_)
            | Transaction::Upload(_)
            | Transaction::Blob(_) => {
                // Do nothing
            }
        }
    }
    Ok(())
}

pub fn copy_to_old_blocks<'a, I, T>(blocks: I, db: &mut T) -> StorageResult<()>
where
    I: Iterator<Item = (&'a BlockHeight, &'a CompressedBlock)>,
    T: OffChainDatabaseTransaction,
{
    for (height, block) in blocks {
        db.storage::<OldFuelBlocks>().insert(height, block)?;
    }
    Ok(())
}

pub fn copy_to_old_block_consensus<'a, I, T>(blocks: I, db: &mut T) -> StorageResult<()>
where
    I: Iterator<Item = (&'a BlockHeight, &'a Consensus)>,
    T: OffChainDatabaseTransaction,
{
    for (height, block) in blocks {
        db.storage::<OldFuelBlockConsensus>()
            .insert(height, block)?;
    }
    Ok(())
}

pub fn copy_to_old_transactions<'a, I, T>(
    transactions: I,
    db: &mut T,
) -> StorageResult<()>
where
    I: Iterator<Item = (&'a TxId, &'a Transaction)>,
    T: OffChainDatabaseTransaction,
{
    for (id, tx) in transactions {
        db.storage::<OldTransactions>().insert(id, tx)?;
    }
    Ok(())
}

#[async_trait::async_trait]
impl<TxStatusManager, BlockImporter, OnChain, OffChain> RunnableService
    for InitializeTask<TxStatusManager, BlockImporter, OnChain, OffChain>
where
    TxStatusManager: ports::worker::TxStatusCompletion,
    BlockImporter: ports::worker::BlockImporter,
    OnChain: ports::worker::OnChainDatabase,
    OffChain: ports::worker::OffChainDatabase,
{
    const NAME: &'static str = "GraphQL_Off_Chain_Worker";
    type SharedData = block_height_subscription::Subscriber;
    type Task = Task<TxStatusManager, OffChain>;
    type TaskParams = ();

    fn shared_data(&self) -> Self::SharedData {
        self.block_height_subscription_handler.subscribe()
    }

    async fn into_task(
        mut self,
        _: &StateWatcher,
        _: Self::TaskParams,
    ) -> anyhow::Result<Self::Task> {
        {
            let db_tx = self.off_chain_database.transaction();
            let total_tx_count = db_tx.get_tx_count().unwrap_or_default();
            graphql_metrics().total_txs_count.set(total_tx_count as i64);
        }

        let balances_indexation_enabled =
            self.off_chain_database.balances_indexation_enabled()?;
        let coins_to_spend_indexation_enabled = self
            .off_chain_database
            .coins_to_spend_indexation_enabled()?;
        let asset_metadata_indexation_enabled = self
            .off_chain_database
            .asset_metadata_indexation_enabled()?;
        tracing::info!(
            balances_indexation_enabled,
            coins_to_spend_indexation_enabled,
            asset_metadata_indexation_enabled,
            "Indexation availability status"
        );
        tracing::debug!(
            balance_query = query_costs().balance_query,
            "Indexation related query costs"
        );

        let InitializeTask {
            chain_id,
            tx_status_manager,
            block_importer,
            blocks_events,
            on_chain_database,
            off_chain_database,
            continue_on_error,
            base_asset_id,
            block_height_subscription_handler,
        } = self;

        let mut task = Task {
            tx_status_manager,
            block_importer: blocks_events,
            database: off_chain_database,
            chain_id,
            continue_on_error,
            balances_indexation_enabled,
            coins_to_spend_indexation_enabled,
            asset_metadata_indexation_enabled,
            base_asset_id,
            block_height_subscription_handler,
        };

        let mut target_chain_height = on_chain_database.latest_height()?;
        // Process all blocks that were imported before the service started.
        // The block importer may produce some blocks on start-up during the
        // genesis stage or the recovery process. In this case, we need to
        // process these blocks because, without them,
        // our block height will be less than on the chain database.
        while let Some(Some(block)) = task.block_importer.next().now_or_never() {
            target_chain_height = Some(*block.sealed_block.entity.header().height());
            task.process_block(block)?;
        }

        sync_databases(&mut task, target_chain_height, &block_importer)?;

        Ok(task)
    }
}

fn sync_databases<TxStatusManager, BlockImporter, OffChain>(
    task: &mut Task<TxStatusManager, OffChain>,
    target_chain_height: Option<BlockHeight>,
    import_result_provider: &BlockImporter,
) -> anyhow::Result<()>
where
    BlockImporter: ports::worker::BlockImporter,
    OffChain: ports::worker::OffChainDatabase,
    TxStatusManager: ports::worker::TxStatusCompletion,
{
    loop {
        let off_chain_height = task.database.latest_height()?;

        if target_chain_height < off_chain_height {
            return Err(anyhow::anyhow!(
                "The target chain height is lower than the off-chain database height"
            ));
        }

        if target_chain_height == off_chain_height {
            break;
        }

        let next_block_height =
            off_chain_height.map(|height| BlockHeight::new(height.saturating_add(1)));

        let next_block_height = match next_block_height {
            Some(block_height) => BlockAt::Specific(block_height),
            None => BlockAt::Genesis,
        };

        let import_result =
            import_result_provider.block_event_at_height(next_block_height)?;

        task.process_block(import_result)?
    }

    Ok(())
}

impl<TxStatusManager, D> RunnableTask for Task<TxStatusManager, D>
where
    D: ports::worker::OffChainDatabase,
    TxStatusManager: ports::worker::TxStatusCompletion,
{
    async fn run(&mut self, watcher: &mut StateWatcher) -> TaskNextAction {
        tokio::select! {
            biased;

            _ = watcher.while_started() => {
                TaskNextAction::Stop
            }

            result = self.block_importer.next() => {
                match result { Some(block) => {
                    let result = self.process_block(block);

                    // In the case of an error, shut down the service to avoid a huge
                    // de-synchronization between on-chain and off-chain databases.
                    match result { Err(e) => {
                        if self.continue_on_error {
                            TaskNextAction::ErrorContinue(e)
                        } else {
                            TaskNextAction::Stop
                        }
                    } _ => {
                        TaskNextAction::Continue
                    }}
                } _ => {
                    TaskNextAction::Stop
                }}
            }
        }
    }

    async fn shutdown(mut self) -> anyhow::Result<()> {
        // Process all remaining blocks before shutdown to not lose any data.
        loop {
            let result = self.block_importer.next().now_or_never();

            match result {
                Some(Some(block)) => {
                    self.process_block(block)?;
                }
                _ => {
                    break;
                }
            }
        }
        Ok(())
    }
}

#[allow(clippy::type_complexity)]
pub(crate) fn new_service<TxStatusManager, BlockImporter, OnChain, OffChain>(
    context: Context<TxStatusManager, BlockImporter, OnChain, OffChain>,
) -> anyhow::Result<
    ServiceRunner<InitializeTask<TxStatusManager, BlockImporter, OnChain, OffChain>>,
>
where
    TxStatusManager: ports::worker::TxStatusCompletion,
    OnChain: ports::worker::OnChainDatabase,
    OffChain: ports::worker::OffChainDatabase,
    BlockImporter: ports::worker::BlockImporter,
{
    let Context {
        tx_status_manager,
        block_importer,
        on_chain_database,
        off_chain_database,
        continue_on_error,
        consensus_parameters,
    } = context;

    let off_chain_block_height = off_chain_database.latest_height()?.unwrap_or_default();

    let service = ServiceRunner::new(InitializeTask {
        tx_status_manager,
        blocks_events: block_importer.block_events(),
        block_importer,
        on_chain_database,
        off_chain_database,
        chain_id: consensus_parameters.chain_id(),
        continue_on_error,
        base_asset_id: *consensus_parameters.base_asset_id(),
        block_height_subscription_handler: block_height_subscription::Handler::new(
            off_chain_block_height,
        ),
    });

    Ok(service)
}
