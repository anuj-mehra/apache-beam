package com.poc.beam.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CompactJsonFile {

    public static void main(String[] args) throws Exception {
        // Path to original formatted file
        String inputPath = "/Users/anujmehra/git/apache-beam/src/main/java/com/poc/beam/json/input-json.json";
        // Path to compact output
        String outputPath = "/Users/anujmehra/git/apache-beam/src/main/java/com/poc/beam/json/output-json.json";

        // Read full file
        String fullJson = new String(Files.readAllBytes(Paths.get(inputPath)));

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(fullJson);

        if (!root.isArray()) {
            throw new RuntimeException("Expected a JSON array at root.");
        }

        // Write compact form
        String compact = mapper.writeValueAsString(root);
        Files.write(Paths.get(outputPath), compact.getBytes());

        System.out.println("✅ Compact file written to: " + outputPath);
    }
}

