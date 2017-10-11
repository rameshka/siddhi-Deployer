package com.wso2;


public class StrQuery {
    String query;
    String strategy;


    public StrQuery(String query, String strategy) {
        this.query = query;
        this.strategy = strategy;
    }

    public StrQuery(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }


    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }
}
