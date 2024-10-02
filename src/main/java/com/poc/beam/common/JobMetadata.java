package com.poc.beam.common;

import java.util.List;

public class JobMetadata {
    private String process_name;
    private List<Step> steps;

    // Getters and Setters
    public String getProcess_name() {
        return process_name;
    }

    public void setProcess_name(String process_name) {
        this.process_name = process_name;
    }

    public List<Step> getSteps() {
        return steps;
    }

    public void setSteps(List<Step> steps) {
        this.steps = steps;
    }
}



