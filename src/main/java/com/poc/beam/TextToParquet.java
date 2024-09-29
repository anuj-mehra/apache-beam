package com.poc.beam;

import com.poc.beam.helper.MyParquetType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;

public class TextToParquet {

    public static void main(String[] args) {
        final Pipeline p = Pipeline.create();

        // Read a text file
        final PCollection<String> lines = p.apply("ReadTextFile", TextIO.read().from("input.txt"));

        // Convert to a Parquet-compatible format (just an example of how you might transform the data)
        final PCollection<MyParquetType> parquetData = lines.apply(
                MapElements.via(new SimpleFunction<String, MyParquetType>() {
                    @Override
                    public MyParquetType apply(String input) {
                        // Assume each line of text represents a record, convert to MyParquetType
                        return new MyParquetType(input);
                    }
                }));

        parquetData.getName();

        // Write to Parquet
        //parquetData.apply("WriteParquetFile", ParquetIO.sink(MyParquetType.class).to("output.parquet"));

        p.run().waitUntilFinish();
    }
}
