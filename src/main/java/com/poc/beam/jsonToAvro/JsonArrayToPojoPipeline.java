package com.poc.beam.jsonToAvro;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.util.List;

public class JsonArrayToPojoPipeline {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Step 1: Read the entire JSON file as a single string
        PCollection<String> jsonContent = pipeline.apply("Read JSON File", TextIO.read().from("/Users/anujmehra/git/apache-beam/src/main/resources/jsonToAvro/input.json"));

        // Step 2: Convert the entire JSON array into POJOs
        PCollection<User> users = jsonContent.apply("Convert JSON to POJOs", ParDo.of(new JsonToUserFn()));

        // Step 3: Print each converted user object
        users.apply("Print Users", ParDo.of(new DoFn<User, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println("Converted User: " + c.element());
            }
        }));

        // Run pipeline
        pipeline.run().waitUntilFinish();
    }

    // Function to convert JSON array to POJOs
    static class JsonToUserFn extends DoFn<String, User> {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        @ProcessElement
        public void processElement(ProcessContext c) {
            try {
                // Parse the JSON string as an array of User objects
                // Combine all lines into a single JSON string if needed
                String json = c.element().replaceAll("\\n", "").replaceAll("\\s{2,}", "");

                // Parse the JSON array into a List of User objects
                List<User> usersList = objectMapper.readValue(json,
                        objectMapper.getTypeFactory().constructCollectionType(List.class, User.class));

                // Output each User object
                for (User user : usersList) {
                    c.output(user);
                }
            } catch (IOException e) {
                System.err.println("Error parsing JSON array: " + c.element());
                e.printStackTrace();
            }
        }
    }
}
