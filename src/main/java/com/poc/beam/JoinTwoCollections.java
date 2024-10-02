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
import org.apache.beam.sdk.values.*;

import java.util.Arrays;
import java.util.List;

public class JoinTwoCollections {

    public static void main(String[] args) {
        final PipelineOptions options = PipelineOptionsFactory.create();
        final Pipeline p = Pipeline.create(options);

        // Define the schema for the records.
        final Schema appSchema1 = Schema.builder()
                .addStringField("trankey")
                .addStringField("accountkey")
                .addStringField("trandate")
                .build();

        // Create a concrete row with that type.
        final Row row = Row.withSchema(appSchema1)
                .addValues("100001", "10000090", "02-OCT-2024")
                .build();

        // Create a source PCollection containing only that row
        final PCollection<Row> inputDf1 =
                PBegin.in(p).apply(Create.of(row).withCoder(RowCoder.of(appSchema1)));


        // Create Dataframe2
        final Schema appSchema2 = Schema.builder()
                .addStringField("trankey")
                .addStringField("trantype")
                .addStringField("periodTypeCode")
                .build();
        final Row row2 = Row.withSchema(appSchema2)
                .addValues("100002", "BUY", "DAILY")
                .build();
        final Row row3 = Row.withSchema(appSchema2)
                .addValues("100001", "BUY", "MTHLY")
                .build();
        final List<Row> rows = Arrays.asList(row2, row3);

        // Create a source PCollection containing only that row
        final PCollection<Row> inputDf2 =
                PBegin.in(p).apply(Create.of(rows).withCoder(RowCoder.of(appSchema2)));

        final PCollectionTuple mergedDf = PCollectionTuple
                .of(new TupleTag<>("df1"), inputDf1)
                .and(new TupleTag<>("df2"), inputDf2);


        final PCollection<Row> joinedDf = mergedDf.apply(
                SqlTransform.query(
                        "SELECT * from df1 left join df2 on df1.trankey = df2.trankey"));


        // Print elements of the PCollection | Print each row in a structured way
        joinedDf.apply(ParDo.of(new DoFn<Row, Void>() {
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
