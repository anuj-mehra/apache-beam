package com.poc.beam.sql;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaFieldSchema.class)
public class Transaction {
    public String id;
    public String customerName;
    public int year;
    public double amount;

    public Transaction() {}

    public Transaction(String id, String customerName, int year, double amount) {
        this.id = id;
        this.customerName = customerName;
        this.year = year;
        this.amount = amount;
    }
}
