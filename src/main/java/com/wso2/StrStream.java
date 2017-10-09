package com.wso2;

public class StrStream {
    String type;
    String definition;

    public StrStream(String type, String definition) {
        this.type = type;
        this.definition = definition;
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
