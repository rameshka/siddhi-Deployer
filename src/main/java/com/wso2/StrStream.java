package com.wso2;

public class StrStream {
    String type;
    String definition;
    String consumptionStrategy;

    public StrStream(String type, String definition) {
        this.type = type;
        this.definition = definition;

    }

    public StrStream(String type, String definition,String consumptionStrategy) {
        this.type = type;
        this.definition = definition;
        this.consumptionStrategy= consumptionStrategy;

    }

    public String getConsumptionStrategy() {
        return consumptionStrategy;
    }

    public void setConsumptionStrategy(String consumptionStrategy) {
        this.consumptionStrategy = consumptionStrategy;
    }

    public String getType() {
        return type;

    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDefinition() {
        return definition;
    }

    public void setDefinition(String definition) {
        this.definition = definition;
    }
}
