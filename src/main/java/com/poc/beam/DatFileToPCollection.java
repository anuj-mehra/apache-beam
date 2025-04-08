package com.poc.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class DatFileToPCollection {

    public static void main(String[] args) throws Exception {
        final String filePath = "/Users/anujmehra/git/apache-beam/src/main/resources/datToAvro/input.dat"; // <-- replace with your .dat file path

        // Step 1: Read header line outside the pipeline
        final List<String> headerColumns;
        try (final BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            final String header = reader.readLine();
            if (header == null) {
                throw new IllegalArgumentException("File is empty!");
            }
            headerColumns = Arrays.asList(header.split("\\|"));
        }

        // Step 2: Build Beam Schema
        final Schema.Builder schemaBuilder = Schema.builder();
        for (final String col : headerColumns) {
            schemaBuilder.addStringField(col.trim()); // all columns treated as String
        }
        final Schema schema = schemaBuilder.build();

        // Step 3: Create Beam pipeline
        final Pipeline pipeline = Pipeline.create();

        // Step 4: Read lines from file
        final PCollection<String> lines = pipeline.apply("ReadFile", TextIO.read().from(filePath));

        // Step 5: Filter out header (matches the full header line)
        final String headerLine = String.join("|", headerColumns);
        final PCollection<String> dataLines = lines.apply("SkipHeader",
                Filter.by((String line) -> !line.equals(headerLine)));

        // Step 6: Convert lines to Row
        final PCollection<Row> rows = dataLines
                .apply("SplitFields", ParDo.of(new DoFn<String, String[]>() {
                    @ProcessElement
                    public void processElement(@Element String line, OutputReceiver<String[]> out) {
                        String[] parts = line.split("\\|");
                        out.output(parts);
                    }
                }))
                .apply("ToRow", MapElements.into(TypeDescriptor.of(Row.class))
                        .via((String[] fields) -> {
                            Row.Builder builder = Row.withSchema(schema);
                            for (final String value : fields) {
                                builder.addValue(value.trim());
                            }
                            return builder.build();
                        }))
                .setRowSchema(schema);

        // Step 7: (Optional) Print to console
        rows.apply("PrintRows", ParDo.of(new DoFn<Row, Void>() {
            @ProcessElement
            public void processElement(@Element Row row) {
                System.out.println(row);
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}

