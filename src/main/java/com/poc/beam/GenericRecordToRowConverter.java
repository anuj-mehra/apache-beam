package com.poc.beam;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenericRecordToRowConverter {

    public static Row convertGenericRecordToRow(GenericRecord record, Schema beamSchema) {
        // Create a Row builder with the provided Beam Schema
        Row.Builder rowBuilder = Row.withSchema(beamSchema);

        // Iterate through all fields in the Beam schema
        for (Schema.Field field : beamSchema.getFields()) {
            String fieldName = field.getName();
            Object avroFieldValue = record.get(fieldName);

            // Handle null values for nullable fields
            if (avroFieldValue == null) {
                rowBuilder.addValue(null);
            } else {
                // Add the value from the GenericRecord to the Row builder
                Schema.FieldType fieldType = field.getType();
                rowBuilder.addValue(convertField(avroFieldValue, fieldType));
            }
        }

        // Build the Row
        return rowBuilder.build();
    }

    // Method to convert a GenericRecord field value to a Beam Row-compatible value
    private static Object convertField(Object avroFieldValue, Schema.FieldType fieldType) {
        switch (fieldType.getTypeName()) {
            case STRING:
                return avroFieldValue.toString();
            case INT32:
                return ((Number) avroFieldValue).intValue();
            case INT64:
                return ((Number) avroFieldValue).longValue();
            case FLOAT:
                return ((Number) avroFieldValue).floatValue();
            case DOUBLE:
                return ((Number) avroFieldValue).doubleValue();
            case BOOLEAN:
                return avroFieldValue;
            case BYTES:
                return avroFieldValue;  // Handle byte arrays
            case ROW:
                // Handle nested records (GenericRecord within GenericRecord)
                return convertGenericRecordToRow((GenericRecord) avroFieldValue, fieldType.getRowSchema());
            case ARRAY:
                // Convert array fields
                return convertArrayField(avroFieldValue, fieldType);
            case MAP:
                // Convert map fields
                return convertMapField(avroFieldValue, fieldType);
            default:
                throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }
    }

    // Convert array fields in GenericRecord to Beam-compatible arrays
    private static Object convertArrayField(Object avroFieldValue, Schema.FieldType elementType) {
        Iterable<?> avroArray = (Iterable<?>) avroFieldValue;
        List<Object> beamArray = new ArrayList<>();
        for (Object element : avroArray) {
            beamArray.add(convertField(element, elementType.getCollectionElementType()));
        }
        return beamArray;
    }

    // Convert map fields in GenericRecord to Beam-compatible maps
    private static Object convertMapField(Object avroFieldValue, Schema.FieldType fieldType) {
        Map<String, ?> avroMap = (Map<String, ?>) avroFieldValue;
        Map<Object, Object> beamMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : avroMap.entrySet()) {
            beamMap.put(entry.getKey(), convertField(entry.getValue(), fieldType.getMapValueType()));
        }
        return beamMap;
    }
}

