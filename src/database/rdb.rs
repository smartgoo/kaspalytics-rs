use std::path::PathBuf;

use rocksdb::{DB, Options, Error};

pub struct Database {
    db: DB,
}

impl Database {
    pub fn new(path: PathBuf) -> Result<Self, Error> {
        let mut options = Options::default();
        options.create_if_missing(true);

        let db = DB::open(&options, path)?;
        Ok(Self { db })
    }
}