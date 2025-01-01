package com.poc.beam.lineage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;

public class LineageTracker {

    final List<Map<String, Object>> lineageLog = new ArrayList<>();

    public void addStep(String stepId,
                        String stepName,
                        String operation,
                        String input,
                        String output,
                        List<String> columnNames,
                        String source) {

        final Map<String, String> columnMappings = new HashMap<>();
        for (final String column : columnNames) {
            columnMappings.put(column, source + "." + column);
        }

        Map<String, Object> stepDetails = new HashMap<>();
        stepDetails.put("stepId", stepId);
        stepDetails.put("stepName", stepName);
        stepDetails.put("operation", operation);
        stepDetails.put("input", input);
        stepDetails.put("output", output);
        stepDetails.put("columnMappings", columnMappings);

        lineageLog.add(stepDetails);
    }

    public String getLineageAsJson() {
        return new JSONObject().put("lineage", lineageLog).toString(4);
    }

}



