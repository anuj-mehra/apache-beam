package com.poc.beam;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.schemas.Schema;

import java.util.ArrayList;
import java.util.List;

public class PCollectionPrinter {

    /**
     * Prints a PCollection<Row> in table format. Handles nested columns by flattening them.
     *
     * @param rows PCollection of Row to print
     */
    public static void printPCollection(PCollection<Row> rows) {
        rows.apply("PrintRows", ParDo.of(new DoFn<Row, Void>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                Row row = context.element();
                List<String> flattenedRow = new ArrayList<>();
                flattenRow(row, flattenedRow, "");

                // Print the flattened row as a formatted table line
                System.out.println(String.join(" | ", flattenedRow));
            }
        }));
    }

    /**
     * Flattens a Row by recursively extracting nested fields into a list of strings.
     *
     * @param row        The Row to flatten
     * @param flattened  The list to store the flattened row values
     * @param parentName Prefix for nested fields
     */
    private static void flattenRow(Row row, List<String> flattened, String parentName) {
        Schema schema = row.getSchema();
        for (Schema.Field field : schema.getFields()) {
            String fieldName = parentName.isEmpty() ? field.getName() : parentName + "." + field.getName();
            Object value = row.getValue(field.getName());

            if (value instanceof Row) {
                // Recursively flatten nested Row
                flattenRow((Row) value, flattened, fieldName);
            } else {
                // Add non-nested field value to the flattened list
                flattened.add(fieldName + ": " + (value != null ? value.toString() : "null"));
            }
        }
    }

    /**
     * Run the pipeline and print the rows.
     *
     * @param rows The PCollection<Row> to print
     * @param result The PipelineResult to wait for
     */
    public static void runAndPrint(PCollection<Row> rows, PipelineResult result) {
        printPCollection(rows);
        result.waitUntilFinish();
    }
}

