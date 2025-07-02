package com.poc.beam.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;

//public class GroupingMultipleJson {

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;

public class GroupingMultipleJson {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Read the file line-by-line
        PCollection<String> lines = pipeline.apply("ReadLines", TextIO.read().from("/Users/anujmehra/git/apache-beam/src/main/java/com/poc/beam/json/input-json.json"));

        lines
                .apply("CompactToOneJsonArray", ParDo.of(new DoFn<String, String>() {
                    private final List<String> buffer = new ArrayList<>();
                    private final ObjectMapper mapper = new ObjectMapper();

                    @ProcessElement
                    public void processElement(@Element String line) {
                        buffer.add(line);
                    }

                    @FinishBundle
                    public void finishBundle(FinishBundleContext context) throws Exception {
                        // Combine all lines to form complete JSON
                        String fullJson = String.join("\n", buffer).trim();

                        // Print to debug if needed
                        System.out.println("DEBUG JSON:\n" + fullJson);

                        // Parse and compact
                        JsonNode root = mapper.readTree(fullJson);

                        if (!root.isArray()) {
                            throw new RuntimeException("Expected a JSON array at root.");
                        }

                        String compact = mapper.writeValueAsString(root);
                        context.output(compact, org.joda.time.Instant.now(), GlobalWindow.INSTANCE);
                    }
                }))
                .apply("WriteOutput", TextIO.write()
                        .to("/Users/anujmehra/git/apache-beam/src/main/java/com/poc/beam/json/output-json.json")
                        .withSuffix(".json")
                        .withNumShards(1));

        pipeline.run().waitUntilFinish();
    }
}
