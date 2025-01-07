package com.poc.beam.lineage;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LineageHelper {

    public static List<String> extractColumnNames(PCollection<GenericRecord> input){

        // Extract the first row using Sample.any(1)
        final PCollection<GenericRecord> firstRow = input.apply("GetFirstRow", Sample.any(1));

        // Log the first row (for demonstration purposes)
        firstRow.apply("LogFirstRow", ParDo.of(new DoFn<GenericRecord, Void>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                GenericRecord record = context.element();
                System.out.println("First Row: " + record);
            }
        }));

        final List<String> columns = new ArrayList<>();
        firstRow.apply("ExtractColumnNames", ParDo.of(new DoFn<GenericRecord, Void>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                final GenericRecord record = context.element();
                final Schema schema = record.getSchema();
                final List<String> columnNames = schema.getFields()
                        .stream()
                        .map(Schema.Field::name)
                        .collect(Collectors.toList());

                // Print column names (or log them)
                System.out.println("Column Names: " + columnNames);
                columns.addAll(columnNames);
            }
        }));

        return columns;
    }


}
