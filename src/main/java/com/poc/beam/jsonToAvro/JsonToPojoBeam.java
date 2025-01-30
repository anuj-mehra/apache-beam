package com.poc.beam.jsonToAvro;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;

public class JsonToPojoBeam {

    public static void main(String[] args) {
        // Step 1: Create a pipeline
        Pipeline pipeline = Pipeline.create();

        // Step 2: Read the JSON file line by line
        PCollection<String> jsonLines = pipeline.apply("Read JSON File", TextIO.read().from("/Users/anujmehra/git/apache-beam/src/main/resources/jsonToAvro/input.json"));

        // Step 3: Convert each line of JSON into a POJO (User)
        PCollection<User> users = jsonLines.apply("Convert JSON to POJO", ParDo.of(new JsonToUserFn()));

        // Step 4: Print the POJOs (for demonstration purposes)
        users.apply("Print Users", ParDo.of(new DoFn<User, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println("Converted User: " + c.element());
            }
        }));

        // Step 5: Run the pipeline
        pipeline.run().waitUntilFinish();
    }


    // Custom DoFn to parse JSON and convert to User POJO
    static class JsonToUserFn extends DoFn<String, User> {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        @ProcessElement
        public void processElement(ProcessContext c) {
            String json = c.element();
            try {
                // Parse the JSON string into a User object
                User user = objectMapper.readValue(json, User.class);
                c.output(user);
            } catch (IOException e) {
                // Handle parsing error
                System.err.println(" Error parsing JSON: " + json);
                e.printStackTrace();
            }
        }
    }
}

