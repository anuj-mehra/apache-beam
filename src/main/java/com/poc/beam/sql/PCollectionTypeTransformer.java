package com.poc.beam.sql;

import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class PCollectionTypeTransformer {

    public static <T> PCollection<Row> convertToRow(PCollection<T> pCollection) {
        return pCollection.apply(Convert.toRows());
    }

    public static <T> PCollection<T> convertFromRow(PCollection<Row> rowCollection, Class<T> targetClass) {
        return rowCollection.apply("Convert Row to " + targetClass.getSimpleName(), Convert.fromRows(targetClass));
    }
}
