use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use sha2::{Digest, Sha256};
use tokio::{fs, spawn};
use wirehair::{WirehairDecoder, WirehairEncoder};

pub struct Store {
    path: PathBuf,
    fragment_size: u32,
    inner_k: u32,
    inner_n: u32,

    new_chunks: Mutex<HashMap<ChunkKey, Arc<WirehairEncoder>>>,
    recovers: Mutex<HashMap<ChunkKey, Arc<Mutex<Option<WirehairDecoder>>>>>,
}

pub type ChunkKey = [u8; 32];

impl Store {
    pub fn new(path: PathBuf, fragment_size: u32, inner_k: u32, inner_n: u32) -> Self {
        Self {
            path,
            fragment_size,
            inner_k,
            inner_n,

            new_chunks: Default::default(),
            recovers: Default::default(),
        }
    }

    pub fn put_new_chunk(&self, chunk: Vec<u8>) -> ChunkKey {
        let key = Sha256::digest(&chunk).into();
        let encoder = WirehairEncoder::new(chunk, self.fragment_size);
        self.new_chunks
            .lock()
            .unwrap()
            .insert(key, Arc::new(encoder));
        key
    }

    pub fn get_new_fragment(&self, key: &ChunkKey, index: u32) -> anyhow::Result<Option<Vec<u8>>> {
        let encoder = if let Some(encoder) = self.new_chunks.lock().unwrap().get(key) {
            encoder.clone()
        } else {
            return Ok(None);
        };
        let mut fragment = vec![0; self.fragment_size as usize];
        encoder.encode(index, &mut fragment)?;
        Ok(Some(fragment))
    }

    pub fn remove_new_chunk(&self, key: &ChunkKey) {
        self.new_chunks.lock().unwrap().remove(key);
    }

    fn chunk_dir(&self, key: &ChunkKey) -> PathBuf {
        // i know there's `hex` and `itertools` in the wild, just want to avoid
        // introduce util dependencies for single use case
        self.path.join(
            key.iter()
                .map(|b| format!("{b:02x}"))
                .reduce(|s1, s2| s1 + &s2)
                .unwrap(),
        )
    }

    pub async fn put_fragment(
        &self,
        key: &ChunkKey,
        index: u32,
        fragment: &[u8],
    ) -> anyhow::Result<()> {
        assert_eq!(fragment.len(), self.fragment_size as usize);
        fs::create_dir(self.chunk_dir(key)).await?;
        fs::write(self.chunk_dir(key).join(format!("{index}")), fragment).await?;
        Ok(())
    }

    pub async fn get_fragment(&self, key: &ChunkKey, index: u32) -> anyhow::Result<Vec<u8>> {
        Ok(fs::read(self.chunk_dir(key).join(format!("{index}"))).await?)
    }

    pub fn recover_chunk(&self, key: &ChunkKey) {
        let decoder = WirehairDecoder::new(
            self.fragment_size as u64 * self.inner_k as u64,
            self.fragment_size,
        );
        self.recovers
            .lock()
            .unwrap()
            .insert(*key, Arc::new(Mutex::new(Some(decoder))));
    }

    pub async fn receive_fragment(
        &self,
        key: &ChunkKey,
        remote_index: u32,
        remote_fragment: &[u8],
        index: u32,
    ) -> anyhow::Result<bool> {
        assert_eq!(remote_fragment.len(), self.fragment_size as usize);
        let decoder_cell = if let Some(decoder) = self.recovers.lock().unwrap().get(key) {
            decoder.clone()
        } else {
            // during waiting recovers lock concurrent decoding has finished recovering
            return Ok(false);
        };
        let mut decoder_cell = decoder_cell.lock().unwrap();
        let Some(decoder) = &mut *decoder_cell else {
            // during waiting decoder lock concurrent decoding has finished recovering
            return Ok(false);
        };
        if !decoder.decode(remote_index, remote_fragment)? {
            return Ok(false);
        }
        self.recovers.lock().unwrap().remove(key);
        let decoder = decoder_cell.take().unwrap();
        let mut fragment = vec![0; self.fragment_size as usize];
        decoder.into_encoder()?.encode(index, &mut fragment)?;
        self.put_fragment(key, index, &fragment).await?;
        Ok(true)
    }
}
