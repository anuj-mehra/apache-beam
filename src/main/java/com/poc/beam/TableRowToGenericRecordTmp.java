package com.poc.beam;

public class TableRowToGenericRecordTmp {

    /*
    Object colJsonStr = row.get("colJson");

if (colJsonStr != null) {
    Map<String, Object> jsonMap = mapper.readValue(colJsonStr.toString(), new TypeReference<Map<String, Object>>() {});
    Schema jsonSchema = schema.getField("colJson").schema().getTypes().get(1); // [null, record]
    GenericRecord jsonRecord = new GenericData.Record(jsonSchema);

    for (Schema.Field field : jsonSchema.getFields()) {
        Object value = jsonMap.get(field.name());

        Schema fieldSchema = field.schema();
        Schema.Type fieldType = fieldSchema.getType();

        // If union (e.g., [null, array] or [null, record])
        if (fieldType == Schema.Type.UNION) {
            List<Schema> types = fieldSchema.getTypes();
            // Skip null type
            fieldSchema = types.stream().filter(s -> s.getType() != Schema.Type.NULL).findFirst().orElse(null);
            fieldType = fieldSchema.getType();
        }

        if (fieldType == Schema.Type.ARRAY) {
            // Handle List<CurrencyCode> (array of records)
            List<Map<String, Object>> list = (List<Map<String, Object>>) value;
            List<GenericRecord> recordList = new ArrayList<>();
            Schema elementType = fieldSchema.getElementType();

            for (Map<String, Object> item : list) {
                GenericRecord nestedRecord = new GenericData.Record(elementType);
                for (Schema.Field nestedField : elementType.getFields()) {
                    nestedRecord.put(nestedField.name(), item.get(nestedField.name()));
                }
                recordList.add(nestedRecord);
            }
            jsonRecord.put(field.name(), recordList);

        } else if (fieldType == Schema.Type.RECORD) {
            // Handle nested object
            Map<String, Object> nestedMap = (Map<String, Object>) value;
            GenericRecord nestedRecord = new GenericData.Record(fieldSchema);
            for (Schema.Field nestedField : fieldSchema.getFields()) {
                nestedRecord.put(nestedField.name(), nestedMap.get(nestedField.name()));
            }
            jsonRecord.put(field.name(), nestedRecord);

        } else {
            // Simple field
            jsonRecord.put(field.name(), value);
        }
    }

    record.put("colJson", jsonRecord);
}

     */
}
