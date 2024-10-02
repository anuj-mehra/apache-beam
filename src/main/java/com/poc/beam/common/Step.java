package com.poc.beam.common;

public class Step {
    private String domain;
    private String dataframeName;
    private String extractType; // for extract step
    private String sourceFileName; // for extract step
    private String columnsToBeRead; // for extract step
    private String dataframesInvolved; // for transform step
    private String sqlQuery; // for transform step
    private String destinationFileName; // for load step

    // Getters and Setters
    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getDataframeName() {
        return dataframeName;
    }

    public void setDataframeName(String dataframeName) {
        this.dataframeName = dataframeName;
    }

    public String getExtractType() {
        return extractType;
    }

    public void setExtractType(String extractType) {
        this.extractType = extractType;
    }

    public String getSourceFileName() {
        return sourceFileName;
    }

    public void setSourceFileName(String sourceFileName) {
        this.sourceFileName = sourceFileName;
    }

    public String getColumnsToBeRead() {
        return columnsToBeRead;
    }

    public void setColumnsToBeRead(String columnsToBeRead) {
        this.columnsToBeRead = columnsToBeRead;
    }

    public String getDataframesInvolved() {
        return dataframesInvolved;
    }

    public void setDataframesInvolved(String dataframesInvolved) {
        this.dataframesInvolved = dataframesInvolved;
    }

    public String getSqlQuery() {
        return sqlQuery;
    }

    public void setSqlQuery(String sqlQuery) {
        this.sqlQuery = sqlQuery;
    }

    public String getDestinationFileName() {
        return destinationFileName;
    }

    public void setDestinationFileName(String destinationFileName) {
        this.destinationFileName = destinationFileName;
    }
}

