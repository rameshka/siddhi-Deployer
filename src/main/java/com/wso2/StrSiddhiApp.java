package com.wso2;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class StrSiddhiApp {

    private Map<String, String> inputStreamMap;
    private Map<String, String> outputStreamMap;
    private List<String> queryList;
    private String appName;


    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public void setInputStream(String key, String inputStream) {
        if (inputStreamMap == null) {

            inputStreamMap = new LinkedHashMap<String, String>();
            inputStreamMap.put(key, inputStream);

        } else {
            inputStreamMap.put(key, inputStream);
        }
    }

    public void setOutputStream(String key, String outputStream) {

        if (outputStreamMap == null) {

            outputStreamMap = new LinkedHashMap<String, String>();
            outputStreamMap.put(key, outputStream);
        } else {
            outputStreamMap.put(key, outputStream);
        }

    }

    public Map<String, String> getInputStreamMap() {
        return inputStreamMap;
    }

    public void setInputStreamMap(Map<String, String> inputStreamMap) {
        this.inputStreamMap = inputStreamMap;
    }

    public Map<String, String> getOutputStreamMap() {
        return outputStreamMap;
    }

    public void setOutputStreamMap(Map<String, String> outputStreamMap) {
        this.outputStreamMap = outputStreamMap;
    }

    public void setQuery(String query) {


        if (queryList == null) {

            queryList = new LinkedList<String>();
            queryList.add(query);
        } else {
            queryList.add(query);
        }
    }


    @Override
    public String toString() {

        StringBuilder stringBuilder = new StringBuilder("@App:name(\"" + appName + "\") \n");
        String s;

        for (Map.Entry<String, String> entry : inputStreamMap.entrySet()) {

            s= entry.getValue();
            if(s!= null){
                stringBuilder.append(entry.getValue()).append(";\n");
            }



        }



        for (Map.Entry<String, String> entry : outputStreamMap.entrySet()) {

            s= entry.getValue();
            if(s!= null){
                stringBuilder.append(entry.getValue()).append(";\n");
            }


        }


        for (int i = 0; i < queryList.size(); i++) {
            stringBuilder.append(queryList.get(i)).append(";\n");
        }

        return stringBuilder.toString();
    }


}
