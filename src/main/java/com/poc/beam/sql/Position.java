package com.poc.beam.sql;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaFieldSchema.class)
public class Position {
    public String id;
    public String symbol;
    public int quantity;
    public String customerId;

    // Default constructor for Beam
    public Position() {}

    public Position(String id, String symbol, int quantity, String customerId) {
        this.id = id;
        this.symbol = symbol;
        this.quantity = quantity;
        this.customerId = customerId;
    }
}

