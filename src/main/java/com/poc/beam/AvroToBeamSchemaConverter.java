package com.poc.beam;



import org.apache.avro.Schema.Field;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.Builder;
//import org.apache.avro.Schema;

public class AvroToBeamSchemaConverter {

    public static Schema convertAvroSchemaToBeamSchema(org.apache.avro.Schema avroSchema) {
        Schema.Builder beamSchemaBuilder = Schema.builder();

        // Iterate through Avro fields and add them to Beam Schema
        for (org.apache.avro.Schema.Field avroField : avroSchema.getFields()) {
            String fieldName = avroField.name();
            org.apache.avro.Schema.Type fieldType = avroField.schema().getType();

            // Convert Avro type to Beam type
            FieldType beamFieldType = convertAvroTypeToBeamType(fieldType, avroField.schema());

            // Add field to the Beam Schema
            beamSchemaBuilder.addField(fieldName, beamFieldType);
        }

        return beamSchemaBuilder.build();
    }

    // Convert Avro type to Beam type
    private static FieldType convertAvroTypeToBeamType(org.apache.avro.Schema.Type avroType, org.apache.avro.Schema fieldSchema) {
        switch (avroType) {
            case STRING:
                return FieldType.STRING;
            case INT:
                return FieldType.INT32;
            case LONG:
                return FieldType.INT64;
            case FLOAT:
                return FieldType.FLOAT;
            case DOUBLE:
                return FieldType.DOUBLE;
            case BOOLEAN:
                return FieldType.BOOLEAN;
            case RECORD:
                // Nested record (Avro schema)
                return FieldType.row(convertAvroSchemaToBeamSchema(fieldSchema));
            case ARRAY:
                // Array field: Convert element type recursively
                return FieldType.array(convertAvroTypeToBeamType(fieldSchema.getElementType().getType(), fieldSchema.getElementType()));
            case MAP:
                // Map field: Convert key (string) and value type recursively
                return FieldType.map(FieldType.STRING, convertAvroTypeToBeamType(fieldSchema.getValueType().getType(), fieldSchema.getValueType()));
            case BYTES:
                return FieldType.BYTES;
            case ENUM:
                return FieldType.STRING; // Enum can be treated as a String in Beam
            case UNION:
                // Handle union type (nullable types, etc.)
                return handleUnionType(fieldSchema);
            default:
                throw new IllegalArgumentException("Unsupported Avro type: " + avroType);
        }
    }

    // Handle Union type (e.g., nullable fields)
    private static FieldType handleUnionType(org.apache.avro.Schema avroSchema) {
        // Check if this is a nullable field (union with NULL)
        if (avroSchema.getTypes().size() == 2 && avroSchema.getTypes().get(0).getType() == Type.NULL) {
            return FieldType.row(convertAvroSchemaToBeamSchema(avroSchema.getTypes().get(1))).withNullable(true);
        } else {
            // Complex union, not just nullable. Handle based on your case.
            throw new IllegalArgumentException("Complex unions are not supported.");
        }
    }
}


