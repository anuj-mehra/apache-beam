package com.poc.beam;

import com.poc.beam.lineage.SchemaLoader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.LinkedList;
import java.util.List;

public class CreatePCollection {

    public static void main(String[] args) {
        final Pipeline pipeline = Pipeline.create();

        final PCollection<Long> numbers = pipeline.apply("CreateNumbers", Create.of(1L, 2L, 3L, 4L, 5L));


        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }
}
