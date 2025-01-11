package com.poc.beam.sql;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;

public class Impl {

    public static void main(String[] args) {
        // Initialize the pipeline
        Pipeline pipeline = Pipeline.create();

        // Example: PCollection for Position
        PCollection<Position> positions = pipeline.apply("Create Positions", Create.of(
                new Position("1", "AAPL", 100, "C1"),
                new Position("2", "GOOG", 200, "C1")
        ));


        // Example: PCollection for Customer
        PCollection<Customer> customers = pipeline.apply("Create Customers", Create.of(
                new Customer("C1", "Alice", "alice@example.com"),
                new Customer("C2", "Bob", "bob@example.com")
        ));

        // Convert each PCollection<T> to PCollection<Row>
        PCollection<Row> positionRows = PCollectionTypeTransformer.convertToRow(positions);
        PCollection<Row> customerRows = PCollectionTypeTransformer.convertToRow(customers);

        // Print schema of rows (optional, for debugging)
        //positionRows.apply("Print Position Rows", org.apache.beam.sdk.transforms.ParDo.of(new PrintRowFn()));
        //customerRows.apply("Print Customer Rows", org.apache.beam.sdk.transforms.ParDo.of(new PrintRowFn()));

        // Run a SQL query
        PCollection<Row> filteredCustomers = customerRows.apply(
                "Filter Customers",
                SqlTransform.query("SELECT customerId, name , email FROM PCOLLECTION WHERE customerId = 'C1'")
        );

        //Convert back to PCollection<Customer>
        PCollection<Customer> customerRowsConverted = PCollectionTypeTransformer.convertFromRow(filteredCustomers, Customer.class);

        customerRowsConverted.apply("Print Customers", org.apache.beam.sdk.transforms.ParDo.of(new DoFn<Customer, Void>() {
            @ProcessElement
            public void processElement(@Element Customer customer) {
                // Print the customer object to the console
                System.out.println(customer);
            }
        }));

        // Perform a SQL JOIN
        PCollection<Row> joinedRows = PCollectionTuple.of("Customers", customerRows)
                .and("Positions", positionRows)
                .apply("Join Customers and Positions",
                        SqlTransform.query(
                                "SELECT " +
                                        "  c.customerId, " +
                                        "  c.name, " +
                                        "  c.email, " +
                                        "  p.id, " +
                                        "  p.symbol, " +
                                        "  p.quantity " +
                                        "FROM Customers c " +
                                        "JOIN Positions p " +
                                        "ON c.customerId = p.customerId"
                        )
                );

        filteredCustomers.apply("Print filtered customer Rows", org.apache.beam.sdk.transforms.ParDo.of(new PrintRowFn()));

        // Print the results
        joinedRows.apply("Print Results", org.apache.beam.sdk.transforms.ParDo.of(new PrintRowFn()));

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }

    // Utility to print rows (for debugging)
    static class PrintRowFn extends org.apache.beam.sdk.transforms.DoFn<Row, Void> {
        @ProcessElement
        public void processElement(@Element Row row) {
            System.out.println(row);
        }
    }
}
