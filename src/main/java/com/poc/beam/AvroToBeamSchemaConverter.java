package com.poc.beam;


import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.Builder;
import org.apache.beam.sdk.schemas.Schema;



public class AvroToBeamSchemaConverter {

    // Method to convert an Avro Schema to a Beam Schema
    public static Schema convertAvroSchemaToBeamSchema(org.apache.avro.Schema avroSchema) {
        Builder beamSchemaBuilder = Schema.builder();

        // Loop through Avro fields and map to Beam Schema
        for (org.apache.avro.Schema.Field avroField : avroSchema.getFields()) {
            String fieldName = avroField.name();
            Schema.FieldType beamFieldType = convertAvroTypeToBeamType(avroField.schema(), false);

            // Add the field to Beam schema
            beamSchemaBuilder.addField(fieldName, beamFieldType);
        }

        return beamSchemaBuilder.build();
    }

    // Helper method to convert Avro field types to Beam field types
    private static Schema.FieldType convertAvroTypeToBeamType(org.apache.avro.Schema avroFieldType, boolean nullable) {
        switch (avroFieldType.getType()) {
            case STRING:
                return FieldType.STRING.withNullable(nullable);
            case INT:
                return FieldType.INT32.withNullable(nullable);
            case LONG:
                return FieldType.INT64.withNullable(nullable);
            case FLOAT:
                return FieldType.FLOAT.withNullable(nullable);
            case DOUBLE:
                return FieldType.DOUBLE.withNullable(nullable);
            case BOOLEAN:
                return FieldType.BOOLEAN.withNullable(nullable);
            case BYTES:
                return FieldType.BYTES.withNullable(nullable);
            case RECORD:
                // If it's a nested record, recursively convert
                Schema nestedSchema = convertAvroSchemaToBeamSchema(avroFieldType);
                return FieldType.row(nestedSchema).withNullable(nullable);
            case ARRAY:
                return FieldType.array(convertAvroTypeToBeamType(avroFieldType.getElementType(), false)).withNullable(nullable);
            case MAP:
                return FieldType.map(FieldType.STRING, convertAvroTypeToBeamType(avroFieldType.getValueType(), false)).withNullable(nullable);
            case UNION:
                // Handle nullable fields (i.e., union with NULL)
                boolean isNullable = avroFieldType.getTypes().stream().anyMatch(t -> t.getType() == org.apache.avro.Schema.Type.NULL);
                for (org.apache.avro.Schema unionType : avroFieldType.getTypes()) {
                    if (unionType.getType() != org.apache.avro.Schema.Type.NULL) {
                        return convertAvroTypeToBeamType(unionType, isNullable);
                    }
                }
                throw new IllegalArgumentException("Invalid UNION type with no non-nullable fields.");
            default:
                throw new IllegalArgumentException("Unsupported Avro type: " + avroFieldType.getType());
        }
    }
}
