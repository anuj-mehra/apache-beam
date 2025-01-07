package com.poc.beam.lineage;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GenerateLineageApacheBeam {

    public static void main(String[] args) throws Exception{

        final LineageTracker lineageTracker = new LineageTracker();
        final Pipeline pipeline = Pipeline.create();

        // Step 1: Read data from inputs
        final org.apache.avro.Schema employeeSchema = SchemaLoader.getSchema("/Users/anujmehra/git/apache-beam/src/main/resources/lineage/avsc/employees.avsc");
        final PCollection<GenericRecord> firstAvro = pipeline.apply("ReadEmployeeData",
                AvroIO.readGenericRecords(employeeSchema)
                        .from("/Users/anujmehra/git/apache-beam/src/main/resources/lineage/data/employees/*.avro"));

        final List<String> columnNames = LineageHelper.extractColumnNames(firstAvro);
        columnNames.forEach(col -> {
            System.out.println(col);
        });

        lineageTracker.addStep("1",
                "ReadEmployeeData",
                "Read",
                "/Users/anujmehra/git/apache-beam/src/main/resources/lineage/data/employees/*.avro",
                "PCollection[Employee]"
                ,columnNames
                ,"EmployeeData");

        System.out.println("---------");

        System.out.println(lineageTracker.getLineageAsJson());

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }

}
