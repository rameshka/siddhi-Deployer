package com.wso2;

import java.util.HashMap;
import java.util.Map;

public class StrQuery {
    String query;
    String strategy;
    private Map<String, StrStream> inputStreamMap;
    private Map<String, StrStream> outputStreamMap;

    public StrQuery(String query, String strategy) {
        this.query = query;
        this.strategy = strategy;
        inputStreamMap = new HashMap<String, StrStream>();
    }

    public void addInputStream(String key, StrStream strStream) {
        inputStreamMap.put(key, strStream);

    }

    public void addOutputStream(String key, StrStream strStream) {

        outputStreamMap.put(key, strStream);

    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Map<String, StrStream> getInputStreamMap() {
        return inputStreamMap;
    }

    public void setInputStreamMap(Map<String, StrStream> inputStreamMap) {
        this.inputStreamMap = inputStreamMap;
    }

    public Map<String, StrStream> getOutputStreamMap() {
        return outputStreamMap;
    }

    public void setOutputStreamMap(Map<String, StrStream> outputStreamMap) {
        this.outputStreamMap = outputStreamMap;
    }

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }
}
