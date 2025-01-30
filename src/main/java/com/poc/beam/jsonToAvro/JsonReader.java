package com.poc.beam.jsonToAvro;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class JsonReader {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Step 1: Read JSON file into PCollection<String>
        PCollection<String> jsonLines = pipeline.apply("Read JSON File", TextIO.read().from("/Users/anujmehra/git/apache-beam/src/main/resources/jsonToAvro/input.json"));

        // Step 2: Print the JSON content to verify
        jsonLines.apply("Print JSON Content", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println("Raw JSON: " + c.element());
            }
        }));

        // Run pipeline
        pipeline.run().waitUntilFinish();
    }
}

