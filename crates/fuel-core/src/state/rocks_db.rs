use crate::{
    database::{
        Error as DatabaseError,
        Result as DatabaseResult,
        convert_to_rocksdb_direction,
        database_description::DatabaseDescription,
    },
    state::IterDirection,
};

use super::rocks_db_key_iterator::{
    ExtractItem,
    RocksDBKeyIterator,
};
use core::ops::Deref;
use fuel_core_metrics::core_metrics::DatabaseMetrics;
use fuel_core_storage::{
    Error as StorageError,
    Result as StorageResult,
    iter::{
        BoxedIter,
        IntoBoxedIter,
        IterableStore,
    },
    kv_store::{
        KVItem,
        KeyItem,
        KeyValueInspect,
        StorageColumn,
        Value,
        WriteOperation,
    },
    transactional::{
        Changes,
        ReferenceBytesKey,
        StorageChanges,
    },
};
use itertools::Itertools;
use rocksdb::{
    BlockBasedOptions,
    BoundColumnFamily,
    Cache,
    ColumnFamilyDescriptor,
    DBAccess,
    DBCompressionType,
    DBRawIteratorWithThreadMode,
    DBWithThreadMode,
    IteratorMode,
    MultiThreaded,
    Options,
    ReadOptions,
    SliceTransform,
    WriteBatch,
};
use std::{
    cmp,
    collections::{
        BTreeMap,
        HashSet,
    },
    fmt,
    fmt::Formatter,
    iter,
    path::{
        Path,
        PathBuf,
    },
    sync::{
        Arc,
        Mutex,
    },
};
use tempfile::TempDir;

#[derive(Debug)]
struct PrimaryInstance(DBWithThreadMode<MultiThreaded>);

impl Deref for PrimaryInstance {
    type Target = DBWithThreadMode<MultiThreaded>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for PrimaryInstance {
    fn drop(&mut self) {
        self.cancel_all_background_work(true);
    }
}

type DB = DBWithThreadMode<MultiThreaded>;

type DropFn = Box<dyn FnOnce() + Send + Sync>;
#[derive(Default)]
struct DropResources {
    // move resources into this closure to have them dropped when db drops
    drop: Option<DropFn>,
}

impl fmt::Debug for DropResources {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "DropResources")
    }
}

impl<F: 'static + FnOnce() + Send + Sync> From<F> for DropResources {
    fn from(closure: F) -> Self {
        Self {
            drop: Option::Some(Box::new(closure)),
        }
    }
}

impl Drop for DropResources {
    fn drop(&mut self) {
        if let Some(drop) = self.drop.take() {
            (drop)()
        }
    }
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
/// Defined behaviour for opening the columns of the database.
pub enum ColumnsPolicy {
    #[cfg_attr(not(feature = "rocksdb-production"), default)]
    // Open a new column only when a database interaction is done with it.
    Lazy,
    #[cfg_attr(feature = "rocksdb-production", default)]
    // Open all columns on creation on the service.
    OnCreation,
}

/// Configuration to create a database
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DatabaseConfig {
    pub cache_capacity: Option<usize>,
    pub max_fds: i32,
    pub columns_policy: ColumnsPolicy,
}

#[cfg(feature = "test-helpers")]
impl DatabaseConfig {
    pub fn config_for_tests() -> Self {
        Self {
            cache_capacity: None,
            max_fds: 512,
            columns_policy: ColumnsPolicy::Lazy,
        }
    }
}

pub struct RocksDb<Description> {
    read_options: ReadOptions,
    db: Arc<PrimaryInstance>,
    block_opts: Arc<BlockBasedOptions>,
    create_family: Option<Arc<Mutex<BTreeMap<String, Options>>>>,
    snapshot: Option<rocksdb::SnapshotWithThreadMode<'static, DB>>,
    metrics: Arc<DatabaseMetrics>,
    // used for RAII
    _drop: Arc<DropResources>,
    _marker: core::marker::PhantomData<Description>,
}

impl<Description> Drop for RocksDb<Description> {
    fn drop(&mut self) {
        // Drop the snapshot before the db.
        // Dropping the snapshot after the db will cause a sigsegv.
        self.snapshot = None;
    }
}

impl<Description> std::fmt::Debug for RocksDb<Description> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RocksDb").field("db", &self.db).finish()
    }
}

impl<Description> RocksDb<Description>
where
    Description: DatabaseDescription,
{
    pub fn default_open_temp() -> DatabaseResult<Self> {
        Self::default_open_temp_with_params(DatabaseConfig {
            cache_capacity: None,
            max_fds: 512,
            columns_policy: ColumnsPolicy::Lazy,
        })
    }

    pub fn default_open_temp_with_params(
        database_config: DatabaseConfig,
    ) -> DatabaseResult<Self> {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path();
        let result = Self::open(
            path,
            enum_iterator::all::<Description::Column>().collect::<Vec<_>>(),
            database_config,
        );
        let mut db = result?;

        db._drop = Arc::new(
            {
                move || {
                    // cleanup temp dir
                    drop(tmp_dir);
                }
            }
            .into(),
        );

        Ok(db)
    }

    pub fn default_open<P: AsRef<Path>>(
        path: P,
        database_config: DatabaseConfig,
    ) -> DatabaseResult<Self> {
        Self::open(
            path,
            enum_iterator::all::<Description::Column>().collect::<Vec<_>>(),
            database_config,
        )
    }

    pub fn prune(path: &Path) -> DatabaseResult<()> {
        let path = path.join(Description::name());
        DB::destroy(&Options::default(), path)
            .map_err(|e| DatabaseError::Other(e.into()))?;
        Ok(())
    }

    pub fn open<P: AsRef<Path>>(
        path: P,
        columns: Vec<Description::Column>,
        database_config: DatabaseConfig,
    ) -> DatabaseResult<Self> {
        Self::open_with(DB::open_cf_descriptors, path, columns, database_config)
    }

    pub fn open_read_only<P: AsRef<Path>>(
        path: P,
        columns: Vec<Description::Column>,
        error_if_log_file_exist: bool,
        database_config: DatabaseConfig,
    ) -> DatabaseResult<Self> {
        Self::open_with(
            |options, primary_path, cfs| {
                DB::open_cf_descriptors_read_only(
                    options,
                    primary_path,
                    cfs,
                    error_if_log_file_exist,
                )
            },
            path,
            columns,
            database_config,
        )
    }

    pub fn open_secondary<PrimaryPath, SecondaryPath>(
        path: PrimaryPath,
        secondary_path: SecondaryPath,
        columns: Vec<Description::Column>,
        database_config: DatabaseConfig,
    ) -> DatabaseResult<Self>
    where
        PrimaryPath: AsRef<Path>,
        SecondaryPath: AsRef<Path>,
    {
        Self::open_with(
            |options, primary_path, cfs| {
                DB::open_cf_descriptors_as_secondary(
                    options,
                    primary_path,
                    secondary_path.as_ref().to_path_buf(),
                    cfs,
                )
            },
            path,
            columns,
            database_config,
        )
    }

    pub fn open_with<F, P>(
        opener: F,
        path: P,
        columns: Vec<Description::Column>,
        database_config: DatabaseConfig,
    ) -> DatabaseResult<Self>
    where
        F: Fn(
            &Options,
            PathBuf,
            Vec<ColumnFamilyDescriptor>,
        ) -> Result<DB, rocksdb::Error>,
        P: AsRef<Path>,
    {
        let original_path = path.as_ref().to_path_buf();
        let path = original_path.join(Description::name());
        let metric_columns = columns
            .iter()
            .map(|column| (column.id(), column.name()))
            .collect::<Vec<_>>();
        let metrics = Arc::new(DatabaseMetrics::new(
            Description::name().as_str(),
            &metric_columns,
        ));
        let mut block_opts = BlockBasedOptions::default();
        // See https://github.com/facebook/rocksdb/blob/a1523efcdf2f0e8133b9a9f6e170a0dad49f928f/include/rocksdb/table.h#L246-L271 for details on what the format versions are/do.
        block_opts.set_format_version(5);

        if let Some(capacity) = database_config.cache_capacity {
            // Set cache size 1/3 of the capacity as recommended by
            // https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#block-cache-size
            let block_cache_size = capacity / 3;
            let cache = Cache::new_lru_cache(block_cache_size);
            block_opts.set_block_cache(&cache);
            // "index and filter blocks will be stored in block cache, together with all other data blocks."
            // See: https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB#indexes-and-filter-blocks
            block_opts.set_cache_index_and_filter_blocks(true);
            // Don't evict L0 filter/index blocks from the cache
            block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
        } else {
            block_opts.disable_cache();
        }
        block_opts.set_bloom_filter(10.0, true);
        block_opts.set_block_size(16 * 1024);

        let mut opts = Options::default();
        opts.set_compression_type(DBCompressionType::Lz4);
        // TODO: Make it customizable https://github.com/FuelLabs/fuel-core/issues/1666
        opts.set_max_total_wal_size(64 * 1024 * 1024);
        let cpu_number =
            i32::try_from(num_cpus::get()).expect("The number of CPU can't exceed `i32`");
        opts.increase_parallelism(cmp::max(1, cpu_number / 2));
        if let Some(capacity) = database_config.cache_capacity {
            // Set cache size 1/3 of the capacity. Another 1/3 is
            // used by block cache and the last 1 / 3 remains for other purposes:
            //
            // https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#block-cache-size
            let row_cache_size = capacity / 3;
            let cache = Cache::new_lru_cache(row_cache_size);
            opts.set_row_cache(&cache);
        }
        opts.set_max_background_jobs(6);
        opts.set_bytes_per_sync(1048576);
        opts.set_max_open_files(database_config.max_fds);

        let existing_column_families = DB::list_cf(&opts, &path).unwrap_or_default();

        let mut cf_descriptors_to_open = BTreeMap::new();
        let mut cf_descriptors_to_create = BTreeMap::new();
        for column in columns.clone() {
            let column_name = Self::col_name(column.id());
            let opts = Self::cf_opts(column, &block_opts);
            if existing_column_families.contains(&column_name) {
                cf_descriptors_to_open.insert(column_name, opts);
            } else {
                cf_descriptors_to_create.insert(column_name, opts);
            }
        }

        if database_config.columns_policy == ColumnsPolicy::OnCreation
            || (database_config.columns_policy == ColumnsPolicy::Lazy
                && cf_descriptors_to_open.is_empty())
        {
            opts.create_if_missing(true);
        }

        let unknown_columns_to_open: BTreeMap<_, _> = existing_column_families
            .iter()
            .filter(|column_name| {
                !cf_descriptors_to_open.contains_key(*column_name)
                    && !cf_descriptors_to_create.contains_key(*column_name)
            })
            .map(|unknown_column_name| {
                let unknown_column_options = Self::default_opts(&block_opts);
                (unknown_column_name.clone(), unknown_column_options)
            })
            .collect();
        cf_descriptors_to_open.extend(unknown_columns_to_open);

        let iterator = cf_descriptors_to_open
            .clone()
            .into_iter()
            .map(|(name, opts)| ColumnFamilyDescriptor::new(name, opts))
            .collect::<Vec<_>>();

        let db = match opener(&opts, path.clone(), iterator) {
            Ok(db) => {
                Ok(db)
            },
            Err(err) => {
                tracing::error!("Couldn't open the database with an error: {}. \nTrying to reopen the database", err);

                let iterator = cf_descriptors_to_open
                    .clone()
                    .into_iter()
                    .map(|(name, opts)| ColumnFamilyDescriptor::new(name, opts))
                    .collect::<Vec<_>>();

                opener(&opts, path.clone(), iterator)
            },
        }
        .map_err(|e| DatabaseError::Other(e.into()))?;

        let create_family = match database_config.columns_policy {
            ColumnsPolicy::OnCreation => {
                for (name, opt) in cf_descriptors_to_create {
                    db.create_cf(name, &opt)
                        .map_err(|e| DatabaseError::Other(e.into()))?;
                }
                None
            }
            ColumnsPolicy::Lazy => Some(Arc::new(Mutex::new(cf_descriptors_to_create))),
        };
        let db = Arc::new(PrimaryInstance(db));

        let rocks_db = RocksDb {
            read_options: Self::generate_read_options(&None),
            block_opts: Arc::new(block_opts),
            snapshot: None,
            db,
            metrics,
            create_family,
            _drop: Default::default(),
            _marker: Default::default(),
        };

        Ok(rocks_db)
    }

    fn generate_read_options(
        snapshot: &Option<rocksdb::SnapshotWithThreadMode<DB>>,
    ) -> ReadOptions {
        let mut opts = ReadOptions::default();
        opts.set_verify_checksums(false);
        if let Some(snapshot) = &snapshot {
            opts.set_snapshot(snapshot);
        }
        opts
    }

    fn read_options(&self) -> ReadOptions {
        Self::generate_read_options(&self.snapshot)
    }

    pub fn create_snapshot(&self) -> Self {
        self.create_snapshot_generic()
    }

    pub fn create_snapshot_generic<TargetDescription>(
        &self,
    ) -> RocksDb<TargetDescription> {
        let db = self.db.clone();
        let block_opts = self.block_opts.clone();
        let create_family = self.create_family.clone();
        let metrics = self.metrics.clone();
        let _drop = self._drop.clone();

        // Safety: We are transmuting the snapshot to 'static lifetime, but it's safe
        // because we are not going to use it after the RocksDb is dropped.
        // We control the lifetime of the `Self` - RocksDb, so we can guarantee that
        // the snapshot will be dropped before the RocksDb.
        #[allow(clippy::missing_transmute_annotations)]
        // Remove this and see for yourself
        let snapshot = unsafe {
            let snapshot = db.snapshot();
            core::mem::transmute(snapshot)
        };
        let snapshot = Some(snapshot);

        RocksDb {
            read_options: Self::generate_read_options(&snapshot),
            block_opts,
            snapshot,
            db,
            create_family,
            metrics,
            _drop,
            _marker: Default::default(),
        }
    }

    fn cf(&self, column: Description::Column) -> Arc<BoundColumnFamily> {
        self.cf_u32(column.id())
    }

    fn cf_u32(&self, column: u32) -> Arc<BoundColumnFamily> {
        let family = self.db.cf_handle(&Self::col_name(column));

        match family {
            None => match &self.create_family {
                Some(create_family) => {
                    let mut lock = create_family
                        .lock()
                        .expect("The create family lock should be available");

                    let name = Self::col_name(column);
                    let Some(family) = lock.remove(&name) else {
                        return self
                            .db
                            .cf_handle(&Self::col_name(column))
                            .expect("No column family found");
                    };

                    self.db
                        .create_cf(&name, &family)
                        .expect("Couldn't create column family");

                    let family = self.db.cf_handle(&name).expect("invalid column state");

                    family
                }
                _ => {
                    panic!("Columns in the DB should have been created on DB opening");
                }
            },
            Some(family) => family,
        }
    }

    fn col_name(column: u32) -> String {
        format!("col-{}", column)
    }

    fn default_opts(block_opts: &BlockBasedOptions) -> Options {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(DBCompressionType::Lz4);
        opts.set_block_based_table_factory(block_opts);

        opts
    }

    fn cf_opts(column: Description::Column, block_opts: &BlockBasedOptions) -> Options {
        let mut opts = Self::default_opts(block_opts);

        // All double-keys should be configured here
        if let Some(size) = Description::prefix(&column) {
            opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(size))
        }

        opts
    }

    /// RocksDB prefix iteration doesn't support reverse order,
    /// so we need to to force the RocksDB iterator to order
    /// all the prefix in the iterator so that we can take the next prefix
    /// as start of iterator and iterate in reverse.
    fn reverse_prefix_iter<T>(
        &self,
        prefix: &[u8],
        column: Description::Column,
    ) -> impl Iterator<Item = StorageResult<T::Item>> + '_ + use<'_, T, Description>
    where
        T: ExtractItem,
    {
        let reverse_iterator = next_prefix(prefix.to_vec()).map(|next_prefix| {
            let mut opts = self.read_options();
            // We need this option because our iterator start in the `next_prefix` prefix section
            // and continue in `prefix` section. Without this option the correct
            // iteration between prefix section isn't guaranteed
            // Source : https://github.com/facebook/rocksdb/wiki/Prefix-Seek#how-to-ignore-prefix-bloom-filters-in-read
            // and https://github.com/facebook/rocksdb/wiki/Prefix-Seek#general-prefix-seek-api
            opts.set_total_order_seek(true);
            self.iterator::<T>(
                column,
                opts,
                IteratorMode::From(next_prefix.as_slice(), rocksdb::Direction::Reverse),
            )
        });

        match reverse_iterator {
            Some(iterator) => {
                let prefix = prefix.to_vec();
                iterator
                    .take_while(move |item| {
                        if let Ok(item) = item {
                            T::starts_with(item, prefix.as_slice())
                        } else {
                            true
                        }
                    })
                    .into_boxed()
            }
            _ => {
                // No next item, so we can start backward iteration from the end.
                let prefix = prefix.to_vec();
                self.iterator::<T>(column, self.read_options(), IteratorMode::End)
                    .take_while(move |item| {
                        if let Ok(item) = item {
                            T::starts_with(item, prefix.as_slice())
                        } else {
                            true
                        }
                    })
                    .into_boxed()
            }
        }
    }

    pub(crate) fn iterator<T>(
        &self,
        column: Description::Column,
        opts: ReadOptions,
        iter_mode: IteratorMode,
    ) -> impl Iterator<Item = StorageResult<T::Item>> + '_ + use<'_, T, Description>
    where
        T: ExtractItem,
    {
        let column_metrics = self.metrics.columns_read_statistic.get(&column.id());

        RocksDBKeyIterator::<_, T>::new(
            self.db.raw_iterator_cf_opt(&self.cf(column), opts),
            iter_mode,
        )
        .map(move |item| {
            item.inspect(|item| {
                self.metrics.read_meter.inc();
                column_metrics.map(|metric| metric.inc());
                self.metrics.bytes_read.inc_by(T::size(item));
            })
            .map_err(|e| DatabaseError::Other(e.into()).into())
        })
    }

    /// The fast way to remove all data from the column.
    pub fn clear_table(&self, column: Description::Column) -> DatabaseResult<()> {
        // Mark column for deletion
        let column_name = Self::col_name(column.id());
        self.db
            .drop_cf(&column_name)
            .map_err(|e| DatabaseError::Other(e.into()))?;

        // Insert new fresh column without data
        let column_name = Self::col_name(column.id());
        let opts = Self::cf_opts(column, self.block_opts.as_ref());
        self.db
            .create_cf(&column_name, &opts)
            .map_err(|e| DatabaseError::Other(e.into()))?;

        Ok(())
    }

    pub fn multi_get<K, I>(
        &self,
        column: u32,
        iterator: I,
    ) -> DatabaseResult<Vec<Option<Vec<u8>>>>
    where
        I: Iterator<Item = K>,
        K: AsRef<[u8]>,
    {
        let column_metrics = self.metrics.columns_read_statistic.get(&column);
        let cl = self.cf_u32(column);
        let results = self
            .db
            .multi_get_cf_opt(iterator.map(|k| (&cl, k)), &self.read_options)
            .into_iter()
            .map(|el| {
                self.metrics.read_meter.inc();
                column_metrics.map(|metric| metric.inc());
                el.map(|value| {
                    value.inspect(|vec| {
                        self.metrics.bytes_read.inc_by(vec.len() as u64);
                    })
                })
                .map_err(|err| DatabaseError::Other(err.into()))
            })
            .try_collect()?;
        Ok(results)
    }

    fn _iter_store<T>(
        &self,
        column: Description::Column,
        prefix: Option<&[u8]>,
        start: Option<&[u8]>,
        direction: IterDirection,
    ) -> BoxedIter<StorageResult<T::Item>>
    where
        T: ExtractItem,
    {
        match (prefix, start) {
            (None, None) => {
                let iter_mode =
                    // if no start or prefix just start iterating over entire keyspace
                    match direction {
                        IterDirection::Forward => IteratorMode::Start,
                        // end always iterates in reverse
                        IterDirection::Reverse => IteratorMode::End,
                    };
                self.iterator::<T>(column, self.read_options(), iter_mode)
                    .into_boxed()
            }
            (Some(prefix), None) => {
                if direction == IterDirection::Reverse {
                    self.reverse_prefix_iter::<T>(prefix, column).into_boxed()
                } else {
                    // start iterating in a certain direction within the keyspace
                    let iter_mode = IteratorMode::From(
                        prefix,
                        convert_to_rocksdb_direction(direction),
                    );

                    // Setting prefix on the RocksDB level to optimize iteration.
                    let mut opts = self.read_options();
                    opts.set_prefix_same_as_start(true);

                    let prefix = prefix.to_vec();
                    self.iterator::<T>(column, opts, iter_mode)
                        // Not all tables has a prefix set, so we need to filter out the keys.
                        .take_while(move |item| {
                            if let Ok(item) = item {
                                T::starts_with(item, prefix.as_slice())
                            } else {
                                true
                            }
                        })
                        .into_boxed()
                }
            }
            (None, Some(start)) => {
                // start iterating in a certain direction from the start key
                let iter_mode =
                    IteratorMode::From(start, convert_to_rocksdb_direction(direction));
                let mut opts = self.read_options();
                // We need this option because our iterator start in the `start` prefix section
                // and continue in next sections. Without this option the correct
                // iteration between prefix section isn't guaranteed
                // Source : https://github.com/facebook/rocksdb/wiki/Prefix-Seek#how-to-ignore-prefix-bloom-filters-in-read
                // and https://github.com/facebook/rocksdb/wiki/Prefix-Seek#general-prefix-seek-api
                opts.set_total_order_seek(true);
                self.iterator::<T>(column, opts, iter_mode).into_boxed()
            }
            (Some(prefix), Some(start)) => {
                // TODO: Maybe we want to allow the `start` to be without a `prefix` in the future.
                // If the `start` doesn't have the same `prefix`, return nothing.
                if !start.starts_with(prefix) {
                    return iter::empty().into_boxed();
                }

                // start iterating in a certain direction from the start key
                // and end iterating when we've gone outside the prefix
                let prefix = prefix.to_vec();
                let iter_mode =
                    IteratorMode::From(start, convert_to_rocksdb_direction(direction));
                self.iterator::<T>(column, self.read_options(), iter_mode)
                    .take_while(move |item| {
                        if let Ok(item) = item {
                            T::starts_with(item, prefix.as_slice())
                        } else {
                            true
                        }
                    })
                    .into_boxed()
            }
        }
    }

    #[cfg(feature = "backup")]
    fn backup_engine<P: AsRef<Path> + ?Sized>(
        backup_dir: &P,
    ) -> DatabaseResult<rocksdb::backup::BackupEngine> {
        use rocksdb::{
            Env,
            backup::{
                BackupEngine,
                BackupEngineOptions,
            },
        };

        let backup_dir = backup_dir.as_ref().join(Description::name());
        let backup_dir_path = backup_dir.as_path();

        let mut backup_engine_options = BackupEngineOptions::new(backup_dir_path)
            .map_err(|e| {
                DatabaseError::BackupEngineInitError(anyhow::anyhow!(
                    "Couldn't create backup engine options for path `{}`: {}",
                    backup_dir_path.display(),
                    e
                ))
            })?;

        let cpu_number =
            i32::try_from(num_cpus::get()).expect("The number of CPU can't exceed `i32`");

        backup_engine_options.set_max_background_operations(cmp::max(1, cpu_number / 4));

        let env = Env::new().map_err(|e| {
            DatabaseError::BackupEngineInitError(anyhow::anyhow!(
                "Couldn't create environment for backup: {}",
                e
            ))
        })?;

        let backup_engine =
            BackupEngine::open(&backup_engine_options, &env).map_err(|e| {
                DatabaseError::BackupEngineInitError(anyhow::anyhow!(
                    "Couldn't open backup engine for path `{}`: {}",
                    backup_dir.display(),
                    e
                ))
            })?;

        Ok(backup_engine)
    }

    #[cfg(feature = "backup")]
    pub fn backup<P: AsRef<Path> + ?Sized>(
        db_dir: &P,
        backup_dir: &P,
    ) -> DatabaseResult<()> {
        let mut backup_engine = Self::backup_engine(backup_dir)?;

        let db_config = DatabaseConfig {
            cache_capacity: None,
            max_fds: -1,
            columns_policy: ColumnsPolicy::Lazy,
        };

        let db = Self::open_read_only(
            db_dir,
            enum_iterator::all::<Description::Column>().collect::<Vec<_>>(),
            false,
            db_config,
        )?;

        backup_engine.create_new_backup(&db.db).map_err(|e| {
            DatabaseError::BackupError(anyhow::anyhow!(
                "Couldn't create new backup for path `{}`: {}",
                backup_dir.as_ref().display(),
                e
            ))
        })?;

        Ok(())
    }

    /// We delegate opening of restored db to consumer, so they can apply their own options
    #[cfg(feature = "backup")]
    pub fn restore<P: AsRef<Path> + ?Sized>(
        db_dir: &P,
        backup_dir: &P,
    ) -> DatabaseResult<()> {
        use rocksdb::backup::RestoreOptions;

        let mut backup_engine = Self::backup_engine(backup_dir)?;
        let restore_option = RestoreOptions::default();

        let db_dir = db_dir.as_ref().join(Description::name());
        let db_dir_path = db_dir.as_path();
        // we use the default wal directory, which is same as db path
        backup_engine
            .restore_from_latest_backup(db_dir_path, db_dir_path, &restore_option)
            .map_err(|e| {
                DatabaseError::RestoreError(anyhow::anyhow!(
                    "Couldn't restore from latest backup for path `{}`: {}",
                    db_dir_path.display(),
                    e
                ))
            })?;

        Ok(())
    }

    pub fn shutdown(&self) {
        while Arc::strong_count(&self.db) > 1 {}
    }
}

pub(crate) struct KeyOnly;

impl ExtractItem for KeyOnly {
    type Item = Vec<u8>;

    fn extract_item<D>(
        raw_iterator: &DBRawIteratorWithThreadMode<D>,
    ) -> Option<Self::Item>
    where
        D: DBAccess,
    {
        raw_iterator.key().map(|key| key.to_vec())
    }

    fn size(item: &Self::Item) -> u64 {
        item.len() as u64
    }

    fn starts_with(item: &Self::Item, prefix: &[u8]) -> bool {
        item.starts_with(prefix)
    }
}

pub(crate) struct KeyAndValue;

impl ExtractItem for KeyAndValue {
    type Item = (Vec<u8>, Value);

    fn extract_item<D>(
        raw_iterator: &DBRawIteratorWithThreadMode<D>,
    ) -> Option<Self::Item>
    where
        D: DBAccess,
    {
        raw_iterator
            .item()
            .map(|(key, value)| (key.to_vec(), Value::from(value)))
    }

    fn size(item: &Self::Item) -> u64 {
        item.0.len().saturating_add(item.1.len()) as u64
    }

    fn starts_with(item: &Self::Item, prefix: &[u8]) -> bool {
        item.0.starts_with(prefix)
    }
}

impl<Description> KeyValueInspect for RocksDb<Description>
where
    Description: DatabaseDescription,
{
    type Column = Description::Column;

    fn size_of_value(
        &self,
        key: &[u8],
        column: Self::Column,
    ) -> StorageResult<Option<usize>> {
        self.metrics.read_meter.inc();
        let column_metrics = self.metrics.columns_read_statistic.get(&column.id());
        column_metrics.map(|metric| metric.inc());

        Ok(self
            .db
            .get_pinned_cf_opt(&self.cf(column), key, &self.read_options)
            .map_err(|e| DatabaseError::Other(e.into()))?
            .map(|value| value.len()))
    }

    fn get(&self, key: &[u8], column: Self::Column) -> StorageResult<Option<Value>> {
        self.metrics.read_meter.inc();
        let column_metrics = self.metrics.columns_read_statistic.get(&column.id());
        column_metrics.map(|metric| metric.inc());

        let value = self
            .db
            .get_cf_opt(&self.cf(column), key, &self.read_options)
            .map_err(|e| DatabaseError::Other(e.into()))?;

        if let Some(value) = &value {
            self.metrics.bytes_read.inc_by(value.len() as u64);
        }

        Ok(value.map(Arc::from))
    }

    fn read(
        &self,
        key: &[u8],
        column: Self::Column,
        offset: usize,
        buf: &mut [u8],
    ) -> StorageResult<bool> {
        self.metrics.read_meter.inc();
        let column_metrics = self.metrics.columns_read_statistic.get(&column.id());
        column_metrics.map(|metric| metric.inc());

        let Some(value) = self
            .db
            .get_pinned_cf_opt(&self.cf(column), key, &self.read_options)
            .map_err(|e| DatabaseError::Other(e.into()))?
        else {
            return Ok(false);
        };

        let bytes_len = value.len();
        let start = offset;
        let buf_len = buf.len();
        let end = offset.saturating_add(buf_len);

        if end > bytes_len {
            return Err(StorageError::Other(anyhow::anyhow!(
                "Offset `{offset}` + buf_len `{buf_len}` read until {end} which is out of bounds `{bytes_len}` for key `{:?}` and column `{column:?}`",
                key
            )));
        }

        let starting_from_offset = &value[start..end];
        buf[..].copy_from_slice(starting_from_offset);

        self.metrics
            .bytes_read
            .inc_by(starting_from_offset.len() as u64);

        Ok(true)
    }
}

impl<Description> IterableStore for RocksDb<Description>
where
    Description: DatabaseDescription,
{
    fn iter_store(
        &self,
        column: Self::Column,
        prefix: Option<&[u8]>,
        start: Option<&[u8]>,
        direction: IterDirection,
    ) -> BoxedIter<KVItem> {
        self._iter_store::<KeyAndValue>(column, prefix, start, direction)
    }

    fn iter_store_keys(
        &self,
        column: Self::Column,
        prefix: Option<&[u8]>,
        start: Option<&[u8]>,
        direction: IterDirection,
    ) -> BoxedIter<KeyItem> {
        self._iter_store::<KeyOnly>(column, prefix, start, direction)
    }
}

impl<Description> RocksDb<Description>
where
    Description: DatabaseDescription,
{
    pub fn commit_changes<'a>(&self, changes: &'a StorageChanges) -> StorageResult<()> {
        let instant = std::time::Instant::now();
        let mut batch = WriteBatch::default();
        let mut conflict_finder = HashSet::<(&'a u32, &'a ReferenceBytesKey)>::new();

        match changes {
            StorageChanges::Changes(changes) => {
                self._populate_batch(&mut batch, &mut conflict_finder, changes)?;
            }
            StorageChanges::ChangesList(changes_list) => {
                for changes in changes_list {
                    self._populate_batch(&mut batch, &mut conflict_finder, changes)?;
                }
            }
        }

        self.db
            .write(batch)
            .map_err(|e| DatabaseError::Other(e.into()))?;
        // TODO: Use `u128` when `AtomicU128` is stable.
        self.metrics.database_commit_time.inc_by(
            u64::try_from(instant.elapsed().as_nanos())
                .expect("The commit shouldn't take longer than `u64`"),
        );

        Ok(())
    }

    fn _populate_batch<'a>(
        &self,
        batch: &mut WriteBatch,
        conflict_finder: &mut HashSet<(&'a u32, &'a ReferenceBytesKey)>,
        changes: &'a Changes,
    ) -> DatabaseResult<()> {
        for (column, ops) in changes {
            let cf = self.cf_u32(*column);
            let column_metrics = self.metrics.columns_write_statistic.get(column);
            for (key, op) in ops {
                self.metrics.write_meter.inc();
                column_metrics.map(|metric| metric.inc());

                if !conflict_finder.insert((column, key)) {
                    return Err(DatabaseError::ConflictingChanges {
                        column: *column,
                        key: key.clone(),
                    });
                }

                match op {
                    WriteOperation::Insert(value) => {
                        self.metrics.bytes_written.inc_by(value.len() as u64);
                        batch.put_cf(&cf, key, value.as_ref());
                    }
                    WriteOperation::Remove => {
                        batch.delete_cf(&cf, key);
                    }
                }
            }
        }
        Ok(())
    }
}

/// The `None` means overflow, so there is not following prefix.
fn next_prefix(mut prefix: Vec<u8>) -> Option<Vec<u8>> {
    for byte in prefix.iter_mut().rev() {
        if let Some(new_byte) = byte.checked_add(1) {
            *byte = new_byte;
            return Some(prefix);
        }
    }
    None
}

#[cfg(feature = "test-helpers")]
pub mod test_helpers {
    use super::*;
    use fuel_core_storage::{
        kv_store::KeyValueMutate,
        transactional::ReadTransaction,
    };

    impl<Description> KeyValueMutate for RocksDb<Description>
    where
        Description: DatabaseDescription,
    {
        fn write(
            &mut self,
            key: &[u8],
            column: Self::Column,
            buf: &[u8],
        ) -> StorageResult<usize> {
            let mut transaction = self.read_transaction();
            let len = transaction.write(key, column, buf)?;
            let changes = transaction.into_changes();
            self.commit_changes(&StorageChanges::Changes(changes))?;

            Ok(len)
        }

        fn delete(&mut self, key: &[u8], column: Self::Column) -> StorageResult<()> {
            let mut transaction = self.read_transaction();
            transaction.delete(key, column)?;
            let changes = transaction.into_changes();
            self.commit_changes(&StorageChanges::Changes(changes))?;
            Ok(())
        }
    }
}

#[allow(non_snake_case)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::database_description::on_chain::OnChain;
    use fuel_core_storage::{
        column::Column,
        kv_store::KeyValueMutate,
    };
    use std::collections::{
        BTreeMap,
        HashMap,
    };
    use tempfile::TempDir;

    fn create_db() -> (RocksDb<OnChain>, TempDir) {
        let tmp_dir = TempDir::new().unwrap();
        (
            RocksDb::default_open(tmp_dir.path(), DatabaseConfig::config_for_tests())
                .unwrap(),
            tmp_dir,
        )
    }

    #[test]
    fn open_new_columns() {
        let tmp_dir = TempDir::new().unwrap();

        // Given
        let old_columns =
            vec![Column::Coins, Column::Messages, Column::UploadedBytecodes];
        let database_with_old_columns = RocksDb::<OnChain>::open(
            tmp_dir.path(),
            old_columns.clone(),
            DatabaseConfig::config_for_tests(),
        )
        .expect("Failed to open database with old columns");
        drop(database_with_old_columns);

        // When
        let mut new_columns = old_columns;
        new_columns.push(Column::ContractsAssets);
        new_columns.push(Column::Metadata);
        let database_with_new_columns = RocksDb::<OnChain>::open(
            tmp_dir.path(),
            new_columns,
            DatabaseConfig::config_for_tests(),
        )
        .map(|_| ());

        // Then
        assert_eq!(Ok(()), database_with_new_columns);
    }

    #[test]
    fn can_put_and_read() {
        let key = vec![0xA, 0xB, 0xC];

        let (mut db, _tmp) = create_db();
        let expected = Value::from([1, 2, 3]);
        db.put(&key, Column::Metadata, expected.clone()).unwrap();

        assert_eq!(db.get(&key, Column::Metadata).unwrap().unwrap(), expected)
    }

    #[test]
    fn put_returns_previous_value() {
        let key = vec![0xA, 0xB, 0xC];

        let (mut db, _tmp) = create_db();
        let expected = Value::from([1, 2, 3]);
        db.put(&key, Column::Metadata, expected.clone()).unwrap();
        let prev = db
            .replace(&key, Column::Metadata, Arc::new([2, 4, 6]))
            .unwrap();

        assert_eq!(prev, Some(expected));
    }

    #[test]
    fn delete_and_get() {
        let key = vec![0xA, 0xB, 0xC];

        let (mut db, _tmp) = create_db();
        let expected = Value::from([1, 2, 3]);
        db.put(&key, Column::Metadata, expected.clone()).unwrap();
        assert_eq!(db.get(&key, Column::Metadata).unwrap().unwrap(), expected);

        db.delete(&key, Column::Metadata).unwrap();
        assert_eq!(db.get(&key, Column::Metadata).unwrap(), None);
    }

    #[test]
    fn key_exists() {
        let key = vec![0xA, 0xB, 0xC];

        let (mut db, _tmp) = create_db();
        let expected = Arc::new([1, 2, 3]);
        db.put(&key, Column::Metadata, expected).unwrap();
        assert!(db.exists(&key, Column::Metadata).unwrap());
    }

    #[test]
    fn commit_changes_inserts() {
        let key = vec![0xA, 0xB, 0xC];
        let value = Value::from([1, 2, 3]);

        let (db, _tmp) = create_db();
        let ops = vec![(
            Column::Metadata.id(),
            BTreeMap::from_iter(vec![(
                key.clone().into(),
                WriteOperation::Insert(value.clone()),
            )]),
        )];

        db.commit_changes(&StorageChanges::Changes(HashMap::from_iter(ops)))
            .unwrap();
        assert_eq!(db.get(&key, Column::Metadata).unwrap().unwrap(), value)
    }

    #[test]
    fn commit_changes_removes() {
        let key = vec![0xA, 0xB, 0xC];
        let value = Arc::new([1, 2, 3]);

        let (mut db, _tmp) = create_db();
        db.put(&key, Column::Metadata, value).unwrap();

        let ops = vec![(
            Column::Metadata.id(),
            BTreeMap::from_iter(vec![(key.clone().into(), WriteOperation::Remove)]),
        )];
        db.commit_changes(&StorageChanges::Changes(HashMap::from_iter(ops)))
            .unwrap();

        assert_eq!(db.get(&key, Column::Metadata).unwrap(), None);
    }

    #[test]
    fn can_use_unit_value() {
        let key = vec![0x00];

        let (mut db, _tmp) = create_db();
        let expected = Value::from([]);
        db.put(&key, Column::Metadata, expected.clone()).unwrap();

        assert_eq!(db.get(&key, Column::Metadata).unwrap().unwrap(), expected);

        assert!(db.exists(&key, Column::Metadata).unwrap());

        assert_eq!(
            db.iter_store(Column::Metadata, None, None, IterDirection::Forward)
                .collect::<Result<Vec<_>, _>>()
                .unwrap()[0],
            (key.clone(), expected.clone())
        );

        assert_eq!(db.take(&key, Column::Metadata).unwrap().unwrap(), expected);

        assert!(!db.exists(&key, Column::Metadata).unwrap());
    }

    #[test]
    fn can_use_unit_key() {
        let key: Vec<u8> = Vec::with_capacity(0);

        let (mut db, _tmp) = create_db();
        let expected = Value::from([1, 2, 3]);
        db.put(&key, Column::Metadata, expected.clone()).unwrap();

        assert_eq!(db.get(&key, Column::Metadata).unwrap().unwrap(), expected);

        assert!(db.exists(&key, Column::Metadata).unwrap());

        assert_eq!(
            db.iter_store(Column::Metadata, None, None, IterDirection::Forward)
                .collect::<Result<Vec<_>, _>>()
                .unwrap()[0],
            (key.clone(), expected.clone())
        );

        assert_eq!(db.take(&key, Column::Metadata).unwrap().unwrap(), expected);

        assert!(!db.exists(&key, Column::Metadata).unwrap());
    }

    #[test]
    fn can_use_unit_key_and_value() {
        let key: Vec<u8> = Vec::with_capacity(0);

        let (mut db, _tmp) = create_db();
        let expected = Value::from([]);
        db.put(&key, Column::Metadata, expected.clone()).unwrap();

        assert_eq!(db.get(&key, Column::Metadata).unwrap().unwrap(), expected);

        assert!(db.exists(&key, Column::Metadata).unwrap());

        assert_eq!(
            db.iter_store(Column::Metadata, None, None, IterDirection::Forward)
                .collect::<Result<Vec<_>, _>>()
                .unwrap()[0],
            (key.clone(), expected.clone())
        );

        assert_eq!(db.take(&key, Column::Metadata).unwrap().unwrap(), expected);

        assert!(!db.exists(&key, Column::Metadata).unwrap());
    }

    #[test]
    fn open_primary_db_second_time_fails() {
        // Given
        let (_primary_db, tmp_dir) = create_db();

        // When
        let columns = enum_iterator::all::<<OnChain as DatabaseDescription>::Column>()
            .collect::<Vec<_>>();
        let result = RocksDb::<OnChain>::open(
            tmp_dir.path(),
            columns,
            DatabaseConfig::config_for_tests(),
        );

        // Then
        assert!(result.is_err());
    }

    #[test]
    fn open_second_read_only_db() {
        // Given
        let (_primary_db, tmp_dir) = create_db();

        // When
        let old_columns =
            vec![Column::Coins, Column::Messages, Column::UploadedBytecodes];
        let result = RocksDb::<OnChain>::open_read_only(
            tmp_dir.path(),
            old_columns.clone(),
            false,
            DatabaseConfig::config_for_tests(),
        )
        .map(|_| ());

        // Then
        assert_eq!(Ok(()), result);
    }

    #[test]
    fn open_secondary_db() {
        // Given
        let (_primary_db, tmp_dir) = create_db();
        let secondary_temp = TempDir::new().unwrap();

        // When
        let old_columns =
            vec![Column::Coins, Column::Messages, Column::UploadedBytecodes];
        let result = RocksDb::<OnChain>::open_secondary(
            tmp_dir.path(),
            secondary_temp.path(),
            old_columns.clone(),
            DatabaseConfig::config_for_tests(),
        )
        .map(|_| ());

        // Then
        assert_eq!(Ok(()), result);
    }

    #[test]
    fn snapshot_allows_get_entry_after_it_was_removed() {
        let (mut db, _tmp) = create_db();
        let value = Value::from([1, 2, 3]);

        // Given
        let key_1 = [1; 32];
        db.put(&key_1, Column::Metadata, value.clone()).unwrap();
        let snapshot = db.create_snapshot();

        // When
        db.delete(&key_1, Column::Metadata).unwrap();

        // Then
        let db_get = db.get(&key_1, Column::Metadata).unwrap();
        assert!(db_get.is_none());

        let snapshot_get = snapshot.get(&key_1, Column::Metadata).unwrap();
        assert_eq!(snapshot_get, Some(value));
    }

    #[test]
    fn snapshot_allows_correct_iteration_even_after_all_elements_where_removed() {
        let (mut db, _tmp) = create_db();
        let value = Value::from([1, 2, 3]);

        // Given
        let key_1 = vec![1; 32];
        let key_2 = vec![2; 32];
        let key_3 = vec![3; 32];
        db.put(&key_1, Column::Metadata, value.clone()).unwrap();
        db.put(&key_2, Column::Metadata, value.clone()).unwrap();
        db.put(&key_3, Column::Metadata, value.clone()).unwrap();
        let snapshot = db.create_snapshot();

        // When
        db.delete(&key_1, Column::Metadata).unwrap();
        db.delete(&key_2, Column::Metadata).unwrap();
        db.delete(&key_3, Column::Metadata).unwrap();

        // Then
        let db_iter = db
            .iter_store(Column::Metadata, None, None, IterDirection::Forward)
            .collect::<Vec<_>>();
        assert!(db_iter.is_empty());

        let snapshot_iter = snapshot
            .iter_store(Column::Metadata, None, None, IterDirection::Forward)
            .collect::<Vec<_>>();
        assert_eq!(
            snapshot_iter,
            vec![
                Ok((key_1, value.clone())),
                Ok((key_2, value.clone())),
                Ok((key_3, value))
            ]
        );
    }

    #[test]
    fn drop_snapshot_after_dropping_main_database_shouldn_panic() {
        let (db, _tmp) = create_db();

        // Given
        let snapshot = db.create_snapshot();

        // When
        drop(db);

        // Then
        drop(snapshot);
    }

    #[test]
    fn open__opens_subset_of_columns_after_opening_all_columns() {
        // Given
        let (first_open_with_all_columns, tmp_dir) = create_db();

        // When
        drop(first_open_with_all_columns);
        let part_of_columns =
            enum_iterator::all::<<OnChain as DatabaseDescription>::Column>()
                .skip(1)
                .collect::<Vec<_>>();
        let open_with_part_of_columns = RocksDb::<OnChain>::open(
            tmp_dir.path(),
            part_of_columns,
            DatabaseConfig::config_for_tests(),
        );

        // Then
        let _ = open_with_part_of_columns
            .expect("Should open the database with shorter number of columns");
    }

    #[test]
    fn iter_store__reverse_iterator__no_target_prefix() {
        // Given
        let (mut db, _tmp) = create_db();
        let value = Value::from([]);
        let key_1 = [1, 1];
        let key_2 = [2, 2];
        let key_3 = [9, 3];
        let key_4 = [10, 0];
        db.put(&key_1, Column::Metadata, value.clone()).unwrap();
        db.put(&key_2, Column::Metadata, value.clone()).unwrap();
        db.put(&key_3, Column::Metadata, value.clone()).unwrap();
        db.put(&key_4, Column::Metadata, value.clone()).unwrap();

        // When
        let db_iter = db
            .iter_store(
                Column::Metadata,
                Some(vec![5].as_slice()),
                None,
                IterDirection::Reverse,
            )
            .map(|item| item.map(|(key, _)| key))
            .collect::<Vec<_>>();

        // Then
        assert_eq!(db_iter, vec![]);
    }

    #[test]
    fn iter_store__reverse_iterator__target_prefix_at_the_middle() {
        // Given
        let (mut db, _tmp) = create_db();
        let value = Value::from([]);
        let key_1 = [1, 1];
        let key_2 = [2, 2];
        let key_3 = [2, 3];
        let key_4 = [10, 0];
        db.put(&key_1, Column::Metadata, value.clone()).unwrap();
        db.put(&key_2, Column::Metadata, value.clone()).unwrap();
        db.put(&key_3, Column::Metadata, value.clone()).unwrap();
        db.put(&key_4, Column::Metadata, value.clone()).unwrap();

        // When
        let db_iter = db
            .iter_store(
                Column::Metadata,
                Some(vec![2].as_slice()),
                None,
                IterDirection::Reverse,
            )
            .map(|item| item.map(|(key, _)| key))
            .collect::<Vec<_>>();

        // Then
        assert_eq!(db_iter, vec![Ok(key_3.to_vec()), Ok(key_2.to_vec())]);
    }

    #[test]
    fn iter_store__reverse_iterator__target_prefix_at_the_end() {
        // Given
        let (mut db, _tmp) = create_db();
        let value = Value::from([]);
        let key_1 = [1, 1];
        let key_2 = [2, 2];
        let key_3 = [2, 3];
        db.put(&key_1, Column::Metadata, value.clone()).unwrap();
        db.put(&key_2, Column::Metadata, value.clone()).unwrap();
        db.put(&key_3, Column::Metadata, value.clone()).unwrap();

        // When
        let db_iter = db
            .iter_store(
                Column::Metadata,
                Some(vec![2].as_slice()),
                None,
                IterDirection::Reverse,
            )
            .map(|item| item.map(|(key, _)| key))
            .collect::<Vec<_>>();

        // Then
        assert_eq!(db_iter, vec![Ok(key_3.to_vec()), Ok(key_2.to_vec())]);
    }

    #[test]
    fn iter_store__reverse_iterator__target_prefix_at_the_end__overflow() {
        // Given
        let (mut db, _tmp) = create_db();
        let value = Value::from([]);
        let key_1 = [1, 1];
        let key_2 = [255, 254];
        let key_3 = [255, 255];
        db.put(&key_1, Column::Metadata, value.clone()).unwrap();
        db.put(&key_2, Column::Metadata, value.clone()).unwrap();
        db.put(&key_3, Column::Metadata, value.clone()).unwrap();

        // When
        let db_iter = db
            .iter_store(
                Column::Metadata,
                Some(vec![255].as_slice()),
                None,
                IterDirection::Reverse,
            )
            .map(|item| item.map(|(key, _)| key))
            .collect::<Vec<_>>();

        // Then
        assert_eq!(db_iter, vec![Ok(key_3.to_vec()), Ok(key_2.to_vec())]);
    }

    #[test]
    fn clear_table__keeps_column_accessible_after_marking_for_deletion() {
        // Given
        let (mut db, _tmp) = create_db();
        let value = Value::from([]);
        let key_1 = [1, 1];
        let key_2 = [255, 254];
        let key_3 = [255, 255];
        db.put(&key_1, Column::Metadata, value.clone()).unwrap();
        db.put(&key_2, Column::Metadata, value.clone()).unwrap();
        db.put(&key_3, Column::Metadata, value.clone()).unwrap();

        // When
        db.clear_table(Column::Metadata).unwrap();

        // Then
        let db_iter = db
            .iter_store_keys(Column::Metadata, None, None, IterDirection::Forward)
            .collect::<Vec<_>>();
        assert_eq!(db_iter, vec![]);
    }
}
