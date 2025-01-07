package com.poc.beam;

import com.poc.beam.lineage.SchemaLoader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class CountOfRowInPCollection {

    public static void main(String[] args) throws Exception {
        final Pipeline pipeline = Pipeline.create();

        // Load the Avro schema
        final org.apache.avro.Schema employeeSchema = SchemaLoader.getSchema("/Users/anujmehra/git/apache-beam/src/main/resources/lineage/avsc/employees.avsc");

        // Read employee data from Avro files
        final PCollection<GenericRecord> firstAvro = pipeline.apply("ReadEmployeeData",
                AvroIO.readGenericRecords(employeeSchema)
                        .from("/Users/anujmehra/git/apache-beam/src/main/resources/lineage/data/employees/*.avro"));

        // Count the records
        final PCollection<Long> count = firstAvro.apply(Count.globally());

        // Convert PCollection<Long> to PCollection<String>
        PCollection<String> countAsString = count.apply("ConvertCountToString",
                MapElements.into(TypeDescriptors.strings())
                        .via(Object::toString));

        // Write the count result to a file
        countAsString.apply("WriteCountToFile", TextIO.write()
                .to("/Users/anujmehra/git/apache-beam/src/main/resources/lineage/output/count")
                .withSuffix(".txt")
                .withoutSharding()); // Ensure a single output file

        /* To print in logs */
        // Convert the count result to a PCollectionView for further usage
        /*final PCollectionView<List<Long>> numbersView = count.apply("ConvertToView", View.asList());

        // Use the side input for demonstration
        pipeline.apply("TriggerViewUsage", Create.of((Void) null)) // Dummy PCollection to trigger the side input usage
                .apply("AccessSideInput", ParDo.of(new DoFn<Void, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                        // Access the list from the side input
                        List<Long> numbersList = context.sideInput(numbersView);
                        System.out.println("Collected Numbers from Side Input: " + numbersList);
                    }
                }).withSideInputs(numbersView));
         */

        // Read the result from the file into memory
        List<String> fileContents = Files.lines(Paths.get("/Users/anujmehra/git/apache-beam/src/main/resources/lineage/output/count.txt"))
                .collect(Collectors.toList());

        // Print the result
        System.out.println("Read from file: " + fileContents.get(0));

        // Run the pipeline
        pipeline.run().waitUntilFinish();


    }
}
