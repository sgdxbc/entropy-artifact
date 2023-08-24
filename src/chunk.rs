use std::{
    collections::HashMap,
    future::Future,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use sha2::{Digest, Sha256};
use tokio::{fs, task::spawn_blocking};
use wirehair::{WirehairDecoder, WirehairEncoder};

use crate::common::hex_string;

pub struct Store {
    path: PathBuf,
    fragment_size: u32,
    inner_k: u32,
    inner_n: u32,

    upload_chunks: HashMap<ChunkKey, Arc<WirehairEncoder>>,
    recovers: HashMap<ChunkKey, Arc<Mutex<Option<WirehairDecoder>>>>,
}

pub type ChunkKey = [u8; 32];

impl Store {
    pub fn new(path: PathBuf, fragment_size: u32, inner_k: u32, inner_n: u32) -> Self {
        Self {
            path,
            fragment_size,
            inner_k,
            inner_n,

            upload_chunks: Default::default(),
            recovers: Default::default(),
        }
    }

    pub fn upload_chunk(&mut self, chunk: Vec<u8>) -> ChunkKey {
        let key = Sha256::digest(&chunk).into();
        let encoder = WirehairEncoder::new(chunk, self.fragment_size);
        self.upload_chunks.insert(key, Arc::new(encoder));
        key
    }

    pub fn generate_fragment(
        &mut self,
        key: &ChunkKey,
        index: u32,
    ) -> impl Future<Output = anyhow::Result<Option<Vec<u8>>>> {
        let encoder = self.upload_chunks[key].clone();
        let fragment_size = self.fragment_size;
        async move {
            spawn_blocking(move || {
                let mut fragment = vec![0; fragment_size as usize];
                encoder.encode(index, &mut fragment)?;
                Ok(Some(fragment))
            })
            .await?
        }
    }

    pub fn finish_upload(&mut self, key: &ChunkKey) {
        self.upload_chunks.remove(key);
    }

    fn chunk_dir(&self, key: &ChunkKey) -> PathBuf {
        // i know there's `hex` and `itertools` in the wild, just want to avoid
        // introduce util dependencies for single use case
        self.path.join(hex_string(&key[..]))
    }

    pub fn put_fragment(
        &self,
        key: &ChunkKey,
        index: u32,
        fragment: Vec<u8>,
    ) -> impl Future<Output = anyhow::Result<()>> {
        assert_eq!(fragment.len(), self.fragment_size as usize);
        let chunk_dir = self.chunk_dir(key);
        async move {
            fs::create_dir(&chunk_dir).await?;
            fs::write(chunk_dir.join(format!("{index}")), fragment).await?;
            Ok(())
        }
    }

    pub fn get_fragment(
        &self,
        key: &ChunkKey,
        index: u32,
    ) -> impl Future<Output = anyhow::Result<Vec<u8>>> {
        let chunk_dir = self.chunk_dir(key);
        async move { Ok(fs::read(chunk_dir.join(format!("{index}"))).await?) }
    }

    pub fn recover_chunk(&mut self, key: &ChunkKey) {
        let decoder = WirehairDecoder::new(
            self.fragment_size as u64 * self.inner_k as u64,
            self.fragment_size,
        );
        self.recovers
            .insert(*key, Arc::new(Mutex::new(Some(decoder))));
    }

    pub async fn accept_fragment(
        &self,
        key: &ChunkKey,
        remote_index: u32,
        remote_fragment: Vec<u8>,
        index: u32,
    ) -> impl Future<Output = anyhow::Result<Option<Vec<u8>>>> {
        assert_eq!(remote_fragment.len(), self.fragment_size as usize);
        let decoder_cell = self.recovers[key].clone();
        let fragment_size = self.fragment_size;
        async move {
            let decoder = spawn_blocking(move || {
                let mut decoder_cell = decoder_cell.lock().unwrap();
                if let Some(decoder) = &mut *decoder_cell {
                    if decoder.decode(remote_index, &remote_fragment)? {
                        return Result::<_, wirehair::WirehairResult>::Ok(Some(
                            decoder_cell.take().unwrap(),
                        ));
                    }
                }
                Ok(None)
            })
            .await??;
            Ok(decoder.map(|decoder| {
                let mut fragment = vec![0; fragment_size as usize];
                // assert success after `decode` returns true
                decoder
                    .into_encoder()
                    .unwrap()
                    .encode(index, &mut fragment)
                    .unwrap();
                fragment
            }))
        }
    }
}
