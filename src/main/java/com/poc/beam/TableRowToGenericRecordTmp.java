package com.poc.beam;

public class TableRowToGenericRecordTmp {

    /*
    import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.*;

public class AvroNestedArrayHandler {

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        // Example JSON string for colJson (simulate row.get("colJson").toString())
        String jsonStr = """
            {
                "fieldNameForListT": [
                    {
                        "name": "item1",
                        "e": {
                            "id": "e1",
                            "code": "USD"
                        }
                    },
                    {
                        "name": "item2",
                        "e": {
                            "id": "e2",
                            "code": "EUR"
                        }
                    }
                ]
            }
            """;

        // Simulate reading Avro schema from .avsc file
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("""
            {
              "type": "record",
              "name": "Parent",
              "fields": [
                {
                  "name": "colJson",
                  "type": ["null", {
                    "type": "record",
                    "name": "ColJsonRecord",
                    "fields": [
                      {
                        "name": "fieldNameForListT",
                        "type": ["null", {
                          "type": "array",
                          "items": {
                            "type": "record",
                            "name": "T",
                            "fields": [
                              { "name": "name", "type": "string" },
                              {
                                "name": "e",
                                "type": {
                                  "type": "record",
                                  "name": "E",
                                  "fields": [
                                    { "name": "id", "type": "string" },
                                    { "name": "code", "type": "string" }
                                  ]
                                }
                              }
                            ]
                          }
                        }]
                      }
                    ]
                  }]
                }
              ]
            }
        """);

        // Parse JSON string to Map
        Map<String, Object> jsonMap = mapper.readValue(jsonStr, new TypeReference<>() {});

        // Build parent record
        GenericRecord parentRecord = new GenericData.Record(schema);
        Schema colJsonSchema = schema.getField("colJson").schema().getTypes().get(1); // [null, record]
        GenericRecord colJsonRecord = new GenericData.Record(colJsonSchema);

        for (Schema.Field field : colJsonSchema.getFields()) {
            Object value = jsonMap.get(field.name());
            Schema fieldSchema = field.schema();
            Schema.Type fieldType = fieldSchema.getType();

            // Handle unions like [null, array]
            if (fieldType == Schema.Type.UNION) {
                fieldSchema = fieldSchema.getTypes().stream()
                        .filter(s -> s.getType() != Schema.Type.NULL)
                        .findFirst().orElse(null);
                fieldType = fieldSchema.getType();
            }

            if (fieldType == Schema.Type.ARRAY) {
                List<Map<String, Object>> listT = (List<Map<String, Object>>) value;
                List<GenericRecord> tRecords = new ArrayList<>();
                Schema tSchema = fieldSchema.getElementType();
                Schema eSchema = tSchema.getField("e").schema();

                // Handle union inside T.e if needed
                if (eSchema.getType() == Schema.Type.UNION) {
                    eSchema = eSchema.getTypes().stream()
                            .filter(s -> s.getType() == Schema.Type.RECORD)
                            .findFirst().orElseThrow();
                }

                for (Map<String, Object> tMap : listT) {
                    GenericRecord tRecord = new GenericData.Record(tSchema);
                    tRecord.put("name", tMap.get("name"));

                    Map<String, Object> eMap = (Map<String, Object>) tMap.get("e");
                    GenericRecord eRecord = new GenericData.Record(eSchema);
                    for (Schema.Field ef : eSchema.getFields()) {
                        eRecord.put(ef.name(), eMap.get(ef.name()));
                    }
                    tRecord.put("e", eRecord);
                    tRecords.add(tRecord);
                }

                colJsonRecord.put(field.name(), tRecords);
            } else {
                colJsonRecord.put(field.name(), value); // simple field
            }
        }

        parentRecord.put("colJson", colJsonRecord);

        // ✅ Done - print final record
        System.out.println(parentRecord);
    }
}

     */
}
