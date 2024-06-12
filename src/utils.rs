use std::collections::HashMap;

use async_channel::{Receiver, Sender};

pub fn string_to_str_hashmap<'a>(hashmap_in: &'a HashMap<String, String>) -> HashMap<&str, &str> {
    hashmap_in
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect::<HashMap<_, _>>()
}

pub fn str_to_string_hashmap(hashmap_in: &HashMap<&str, &str>) -> HashMap<String, String> {
    hashmap_in
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect::<HashMap<_, _>>()
}

pub fn new_chan<T>(buffer_size: u32) -> (Sender<T>, Receiver<T>) {
    if buffer_size == 0 {
        async_channel::unbounded::<T>()
    } else {
        async_channel::bounded::<T>(usize::try_from(buffer_size).expect("chunk size too large"))
    }
}
