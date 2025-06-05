package com.poc.beam.datToAvro;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaFieldSchema.class)
public class MyRecord {
    public String FSI;
    public String Account_Number;
    public String amount;

    // No-arg constructor needed for Beam
    public MyRecord() {}

    public MyRecord(String FSI, String Account_Number, String amount) {
        this.FSI = FSI;
        this.Account_Number = Account_Number;
        this.amount = amount;
    }
}

