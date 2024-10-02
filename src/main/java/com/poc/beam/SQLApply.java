package com.poc.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class SQLApply {

    public static void main(String[] args) {

        final PipelineOptions options = PipelineOptionsFactory.create();
        final Pipeline p = Pipeline.create(options);

        // Define the schema for the records.
        final Schema appSchema = Schema.builder()
                        //.addInt32Field("trankey")
                        .addStringField("trankey")
                        .addStringField("accountkey")
                        .addStringField("trandate")
                        //.addDateTimeField("trandate")
                        .build();

        // Create a concrete row with that type.
        final Row row = Row.withSchema(appSchema)
                .addValues("100001", "10000090", "02-OCT-2024")
                .build();

        // Create a source PCollection containing only that row
        final PCollection<Row> inputDf =
                PBegin.in(p).apply(Create.of(row).withCoder(RowCoder.of(appSchema)));

        final PCollection<Row> filteredNames = inputDf.apply(
                SqlTransform.query(
                        "SELECT trankey, accountkey, trandate "
                                + "FROM PCOLLECTION "
                                + "WHERE trankey=100001"));

        // Print elements of the PCollection | Print each row in a structured way
        filteredNames.apply(ParDo.of(new DoFn<Row, Void>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                Row row = context.element();

                // Print the Row schema
                Schema schema = row.getSchema();
                StringBuilder rowOutput = new StringBuilder();

                // Iterate over each field in the row and print its name and value
                schema.getFields().forEach(field -> {
                    String fieldName = field.getName();
                    String fieldValue = row.getValue(field.getName());
                    rowOutput.append(fieldName)
                            .append(": ")
                            .append(fieldValue)
                            .append(", ");
                });

                // Remove the trailing comma and space
                if (rowOutput.length() > 0) {
                    rowOutput.setLength(rowOutput.length() - 2);
                }

                // Print the formatted row
                System.out.println(rowOutput.toString());
            }
        }));

        p.run().waitUntilFinish();

    }
}
