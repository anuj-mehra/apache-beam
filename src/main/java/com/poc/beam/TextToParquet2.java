package com.poc.beam;

import com.poc.beam.helper.MyParquetType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class TextToParquet2 {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        // Read a text file
        final PCollection<String> lines = p.apply("ReadTextFile", TextIO.read().from("/Users/anujmehra/git/apache-beam/src/main/resources/input.txt"));

        // Convert to a Parquet-compatible format (just an example of how you might transform the data)
        final PCollection<MyParquetType> parquetData = lines.apply(
                MapElements.via(new SimpleFunction<String, MyParquetType>() {
                    @Override
                    public MyParquetType apply(String input) {
                        // Assume each line of text represents a record, convert to MyParquetType
                        return new MyParquetType(input);
                    }
                }));

        // Write to Parquet
        //parquetData.apply("WriteParquetFile", ParquetIO.sink(MyParquetType.class).to("output.parquet"));

        p.run().waitUntilFinish();
    }
}
