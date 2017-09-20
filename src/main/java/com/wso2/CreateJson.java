package com.wso2;

import net.minidev.json.JSONArray;
import org.json.simple.JSONObject;


import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class CreateJson {



    public void writeConfiguration(List<StrSiddhiApp> strSiddhiAppList,String deploymentJSONURI) throws IOException {




        JSONArray jsonArray = new JSONArray();
        JSONObject jsonObject;

        for (StrSiddhiApp strSiddhiApp:strSiddhiAppList){
            jsonObject = new JSONObject();
            jsonObject.put("name",strSiddhiApp.getAppName());
            jsonObject.put("parallel",strSiddhiApp.getParallel());
            jsonObject.put("app",strSiddhiApp.toJsonString());

            jsonArray.add(jsonObject);

        }

        jsonObject = new JSONObject();
        jsonObject.put("name","SiddhiApp");
        jsonObject.put("siddhiApps",jsonArray);

        String removeEscape = jsonObject.toJSONString().replaceAll("\\\\","");

        FileWriter fileWriter = new FileWriter(deploymentJSONURI);
        fileWriter.write(removeEscape);

        fileWriter.flush();

    }
}
