package com.poc.beam.common;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class YamlReader {

    public static JobMetadata readYml(String yamlFilePath) throws IOException {
        // Load YAML file as InputStream
        try (InputStream inputStream = new FileInputStream(yamlFilePath)) {
            // Create a Yaml instance
            final Yaml yaml = new Yaml();
            final YamlWrapper yamlWrapper = yaml.loadAs(inputStream, YamlWrapper.class); // Use JobConfig instead of JobMetadata

            // Access parsed data
            final JobMetadata jobMetadata = yamlWrapper.getJob_metadata();

            // Access parsed data
            System.out.println("Process Name: " + jobMetadata.getProcess_name());
            for (Step step : jobMetadata.getSteps()) {
                System.out.println("Step Domain: " + step.getDomain());
                System.out.println("DataFrame Name: " + step.getDataframeName());
                System.out.println("Source File: " + step.getSourceFileName());
                System.out.println("SQL Query: " + step.getSqlQuery());
                System.out.println("Destination File: " + step.getDestinationFileName());
                System.out.println();
            }
            return jobMetadata;
        }
    }

    public static void main(String[] args) {

        try {
            YamlReader.readYml("/Users/anujmehra/git/apache-beam/src/main/resources/yml/tokyo-cash-tran-processor.yaml");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

