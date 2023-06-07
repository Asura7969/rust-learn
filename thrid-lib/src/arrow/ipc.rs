#[warn(unused_imports)]
#[cfg(test)]
mod tests {
    use std::{io::BufReader, sync::Arc};

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_cast::pretty::print_batches;
    use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
    use arrow_schema::{DataType, Field, Schema};
    use tempfile::NamedTempFile;

    // https://github.com/apache/arrow-rs/blob/0c002e20f66c3ab53b883f3b7fb1a72a2e903a73/arrow-integration-testing/src/bin/arrow-stream-to-file.rs
    #[test]
    fn test_ipc_write_read() {
        let file = NamedTempFile::new().unwrap();
        let read_file = file.reopen().unwrap();

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let mut stream_writer = StreamWriter::try_new(file, &schema).unwrap();

        let ids = Int32Array::from(vec![1, 2, 3, 4]);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids)]).unwrap();

        stream_writer.write(&batch).unwrap();
        stream_writer.finish().unwrap();

        let reader = BufReader::new(read_file);
        let arrow_stream_reader = StreamReader::try_new(reader, None).unwrap();
        let schema = arrow_stream_reader.schema();
        println!("arrow stream reader schema is: {}", schema);

        for ele in arrow_stream_reader.into_iter() {
            print_batches(&[ele.unwrap()]).unwrap();
        }
    }
}
