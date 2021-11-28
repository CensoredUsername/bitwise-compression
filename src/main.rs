use entropy_compression::{ParallelEncoder, ParallelDecoder};
use byteorder::{ByteOrder, LittleEndian};
use std::time::Instant;

fn main() {
    let mut args = std::env::args();
    let _ = args.next().expect("what");
    let src = args.next().expect("no source directory");
    let dst = args.next().expect("no dest directory");
    if let Some(_) = args.next() {
        panic!("Too many arguments");
    }

    for entry in std::fs::read_dir(src.as_str()).unwrap() {
        let src_path = entry.unwrap().path();

        println!("file: {:?}", src_path);

        let src_data_raw = std::fs::read(&src_path).unwrap();

        let mut src_data = vec![0; src_data_raw.len() / std::mem::size_of::<u64>()];
        LittleEndian::read_u64_into(&src_data_raw, &mut src_data);

        // start time
        let start_time = Instant::now();

        // encode stuff
        let mut encoder = ParallelEncoder::<u64>::new();
        for number in &src_data {
            encoder.feed(number);
        }
        encoder.flush();

        // serialize
        let mut compressed = Vec::new();
        encoder.serialize(&mut compressed);

        // done compressing
        let compressed_time = Instant::now();

        // decode to check the data matches
        let mut decoder = ParallelDecoder::<u64>::deserialize(&mut compressed.as_slice()).unwrap();
        let mut decoded = Vec::new();
        for _ in &src_data {
            decoded.push(decoder.get());
        }

        // done decoding
        let decoded_time = Instant::now();

        println!("compressed in {:?}, decompressed in {:?}", compressed_time - start_time, decoded_time - compressed_time);

        assert_eq!(decoded, src_data);

        // write the compressed data to a file
        let mut dst_path = std::path::PathBuf::from(&dst);
        dst_path.push(src_path.file_name().unwrap());
        std::fs::write(&dst_path, &compressed).unwrap();
    }
}