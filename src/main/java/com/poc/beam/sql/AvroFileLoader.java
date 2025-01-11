package com.poc.beam.sql;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class AvroFileLoader {

    public void saveAsAvro(PCollection<Row> input){

        /*// Convert PCollection<Row> to PCollection<GenericRecord>
        PCollection<GenericRecord> avroRecords = input.apply("Convert to GenericRecord",
                MapElements.via(new SimpleFunction<Row, GenericRecord>() {
                    @Override
                    public GenericRecord apply(Row row) {
                        return AvroUtils.toGenericRecord(row, avroSchema);
                    }
                }));

        // Write GenericRecord to Avro file
        avroRecords.apply("Write to Avro", FileIO.<GenericRecord>write()
                .to("output/path/customers") // Replace with your desired output path
                .withSuffix(".avro")
                .withNumShards(1) // Adjust number of shards as needed
                .via(AvroIO.sinkViaGenericRecords(avroSchema)));
*/

    }
}
