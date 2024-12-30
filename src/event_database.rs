use std::{collections::HashMap, error::Error};

use serde::de::DeserializeOwned;

#[trait_variant::make(Send)]
pub trait EventDatabase<T: DeserializeOwned> {
    async fn read(
        &self,
        query_name: &str,
        query: &str,
        params: Option<HashMap<String, String>>,
    ) -> Result<T, Box<dyn Error>>;
    async fn write(
        &mut self,
        query: &str,
        params: Option<HashMap<String, String>>,
    ) -> Option<Box<dyn Error>>;
    async fn batch_write(
        &mut self,
        queries: Vec<&str>,
        params: Vec<Option<HashMap<String, String>>>,
    ) -> Option<Box<dyn Error>>;
    async fn chunk_write(
        &mut self,
        query: &str,
        params: Vec<HashMap<String, &[HashMap<String, String>]>>,
        chunk_size: usize,
    ) -> Option<Box<dyn Error>>;
    async fn batch_read(
        &mut self,
        queries: Vec<&str>,
        params: Vec<Option<HashMap<String, String>>>,
    ) -> Result<Vec<T>, Box<dyn Error>>;
}