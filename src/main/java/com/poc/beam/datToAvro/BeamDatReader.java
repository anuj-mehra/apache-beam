package com.poc.beam.datToAvro;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.*;

public class BeamDatReader {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        Schema schema = Schema.builder()
                .addStringField("FSI")
                .addStringField("Account_Number")
                .addStringField("amount")
                .build();

        // 1. Read lines
        PCollection<String> lines = p.apply("ReadFile", TextIO.read().from("/Users/anujmehra/git/apache-beam/src/main/resources/datToAvro/input.dat"));

        // 2. Remove header
        PCollection<String> dataLines = lines.apply("SkipHeader", ParDo.of(new DoFn<String, String>() {
            private boolean isHeader = true;

            @ProcessElement
            public void processElement(@Element String line, OutputReceiver<String> out) {
                if (isHeader) {
                    isHeader = false;
                } else {
                    out.output(line);
                }
            }
        }));

        // 3. Convert to Rows
        PCollection<Row> rowCollection = dataLines.apply("ToRow", MapElements
                .into(TypeDescriptor.of(Row.class))
                .via((String line) -> {
                    String[] parts = line.split("\\|");
                    return Row.withSchema(schema)
                            .addValues(parts[0], parts[1], parts[2])
                            .build();
                })).setRowSchema(schema);

        // 4. Convert to Custom Type (POJO)
        PCollection<MyRecord> pojos = rowCollection.apply("ToPojo", Convert.fromRows(MyRecord.class));

        // 5. Print for debug
        pojos.apply("Print", ParDo.of(new DoFn<MyRecord, Void>() {
            @ProcessElement
            public void processElement(@Element MyRecord record, OutputReceiver<Void> out) {
                System.out.println("Record: " + record.FSI + " | " + record.Account_Number + " | " + record.amount);
            }
        }));

        p.run().waitUntilFinish();
    }
}

