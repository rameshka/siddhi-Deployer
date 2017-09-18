package com.wso2;


import org.apache.log4j.Logger;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.api.execution.ExecutionElement;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.util.ExceptionUtil;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class SiddhiDeployer {

    private int[] queryContextStartIndex;
    int[] queryContextEndIndex;
    private Map<String, StrSiddhiApp> distributiveMap;
    private SiddhiAppRuntime siddhiAppRuntime;


    private static final Logger log = Logger.getLogger(SiddhiDeployer.class);


    public SiddhiDeployer() {
        this.distributiveMap = new LinkedHashMap<String, StrSiddhiApp>();
    }

    public static void main(String[] args) {


        SiddhiDeployer siddhiDeployer = new SiddhiDeployer();


        String siddhiAppString = "@App:name(\"SmartHomePlan\") \n" +
                "@Source(type = 'tcp', context='SmartHomeData',\n" +
                "@map(type='binary')) " +
                "define stream SmartHomeData (id string, value float, property bool, plugId int, householdId int, houseId int, currentTime string); " +
                "@Sink(type='tcp', url='tcp://localhost:9893/OutputStream',context='OutputStream', port='9893'," +
                "@map(type='binary')) " +
                "define stream OutputStream (houseId int, maxVal float, minVal float, avgVal double); " +
                "@info(name = 'query1') @dist(execGroup='group1')" +
                "from SmartHomeData " +
                "select houseId as houseId, max(value) as maxVal, min(value) as minVal, avg(value) as avgVal group by houseId " +
                "insert into UsageStream; " +
                "@info(name = 'query2') @dist(execGroup='group2')" +
                "from UsageStream " +
                "select houseId, maxVal, minVal, avgVal " + "insert into OutputStream;";

        String siddhiAppString1 = "@App:name(\"SmartHomePlan\") " +
                "@Source(type = 'tcp', context='StockStream',@map(type='binary')) " +
                "define stream StockStream (symbol string, price float, volume long);" +

                "define stream CheckStockStream (symbol string, volume long);" +

                "@from(eventtable = 'rdbms' ,datasource.name = 'cepDB' , table.name = 'stockInfo' , bloom.filters = 'enable')" +
                "define table StockTable (symbol string, price float, volume long);" +

                "@info(name = 'query1') @dist(execGroup='group1')" +
                "from StockStream " +
                "insert into StockTable ;" +

                "@info(name = 'query2') @dist(execGroup='group2')" + "from CheckStockStream[(StockTable.symbol==symbol) in StockTable]" + "insert into OutStream;";


        String siddhiAppString2 = "@App:name(\"SmartHomePlan\") \n" +
                "@Source(type = 'tcp', context='TempStream',\n" +
                "@map(type='binary')) " +
                "define stream TempStream(deviceID long, roomNo int, temp double);\n" +
                "define stream RegulatorStream(deviceID long, roomNo int, isOn bool);\n" +
                "define window TempWindow(deviceID long, roomNo int, temp double) time(1 min);\n" +
                " \n" +
                "@info(name = 'query1') @dist(execGroup='group1')" +
                "from TempStream[temp > 30.0]\n" +
                "insert into TempWindow;\n" +
                " \n" +
                "@info(name = 'query2') @dist(execGroup='group2')" +
                "from TempWindow\n" +
                "join RegulatorStream[isOn == false]#window.length(1) as R\n" +
                "on TempWindow.roomNo == R.roomNo\n" +
                "select TempWindow.roomNo, R.deviceID, 'start' as action\n" +
                "insert into RegulatorActionStream;";


        String siddhiApp3 = "";


        siddhiDeployer.DistributeSiddiApp(siddhiAppString);
        siddhiDeployer.connectApplications();


    }
    //TODO:if its a window parallelism in not allowed


    public void DistributeSiddiApp(String siddhiAppString) {

        StrSiddhiApp siddhiAppdist;
        List<String> listInputStream;
        SiddhiApp siddhiApp = SiddhiCompiler.parse(siddhiAppString);

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppString);

        String groupName;

        for (ExecutionElement executionElement : siddhiApp.getExecutionElementList()) {

            groupName = null;

            for (int i = 0; i < executionElement.getAnnotations().size(); i++) {
                if (executionElement.getAnnotations().get(i).getElement("execGroup") != null) {
                    groupName = executionElement.getAnnotations().get(i).getElement("execGroup");

                }

            }

            String inputStreamDefinition;
            String outputStreamDefinition;

            //if exection element is a query
            if (executionElement instanceof Query) {

                if (groupName != null && !distributiveMap.containsKey(groupName)) {

                    siddhiAppdist = new StrSiddhiApp();
                    siddhiAppdist.setAppName(groupName);
                } else if (distributiveMap.containsKey(groupName)) {
                    siddhiAppdist = distributiveMap.get(groupName);

                } else {
                    //will work if execGroup is not mentioned-those will go to a single app
                    siddhiAppdist = new StrSiddhiApp();

                    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                    siddhiAppdist.setAppName(Long.toString(timestamp.getTime()));
                }


                listInputStream = ((Query) executionElement).getInputStream().getAllStreamIds();

                for (int j = 0; j < listInputStream.size(); j++) {
                    String inputStreamId = listInputStream.get(j);
                    inputStreamDefinition = returnStreamDefinition(inputStreamId, siddhiApp, siddhiAppString);


                    siddhiAppdist.setInputStream(inputStreamId, inputStreamDefinition);

                }

                String outputStreamId = ((Query) executionElement).getOutputStream().getId();
                outputStreamDefinition = returnStreamDefinition(outputStreamId, siddhiApp, siddhiAppString);

                siddhiAppdist.setOutputStream(outputStreamId, outputStreamDefinition);


                //query taken
                queryContextStartIndex = ((Query) executionElement).getQueryContextStartIndex();
                queryContextEndIndex = ((Query) executionElement).getQueryContextEndIndex();
                String strQuery = ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString);


                siddhiAppdist.setQuery(strQuery);

                distributiveMap.put(groupName, siddhiAppdist);


            }


        }

    }


    private String returnStreamDefinition(String streamId, SiddhiApp siddhiApp, String siddhiAppString) {

        String streamDefinition = null;

        if (siddhiApp.getStreamDefinitionMap().containsKey(streamId)) {

            queryContextStartIndex = siddhiApp.getStreamDefinitionMap().get(streamId).getQueryContextStartIndex();
            queryContextEndIndex = siddhiApp.getStreamDefinitionMap().get(streamId).getQueryContextEndIndex();
            streamDefinition = ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString);


        } else if (siddhiApp.getTableDefinitionMap().containsKey(streamId)) {

            queryContextStartIndex = siddhiApp.getTableDefinitionMap().get(streamId).getQueryContextStartIndex();
            queryContextEndIndex = siddhiApp.getTableDefinitionMap().get(streamId).getQueryContextEndIndex();
            streamDefinition = ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString);

        } else if (siddhiApp.getWindowDefinitionMap().containsKey(streamId)) {


            queryContextStartIndex = siddhiApp.getWindowDefinitionMap().get(streamId).getQueryContextStartIndex();
            queryContextEndIndex = siddhiApp.getWindowDefinitionMap().get(streamId).getQueryContextEndIndex();
            streamDefinition = ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString);


        }

        return streamDefinition;

    }


    private void connectApplications()

    {
        List<String> stream;
        List<StrSiddhiApp> listSiddhiApps = new ArrayList<StrSiddhiApp>(distributiveMap.values());

        for (int i = 0; i < listSiddhiApps.size(); i++) {

            stream = new ArrayList<String>(listSiddhiApps.get(i).getOutputStreamMap().keySet());

            for (int j = i + 1; j < listSiddhiApps.size(); j++) {


                //TODO:check if outputStream can be only 1

                if (listSiddhiApps.get(j).getInputStreamMap().containsKey(stream.get(0))) {
                    if (listSiddhiApps.get(j).getInputStreamMap().containsKey(stream.get(0))) {


                        String definition = returnStream(stream.get(0), listSiddhiApps.get(i));
                        StringBuilder stringBuilder1 = new StringBuilder("@Sink(type = 'tcp',url='tcp://${" + listSiddhiApps.get(i).getAppName() + " sink_ip}:{" + listSiddhiApps.get(i).getAppName() + " sink_port}/" + stream.get(0) + "\'" + ", context=\'" + stream.get(0) + "\',@map(type='binary'))").append(definition);

                        listSiddhiApps.get(i).getOutputStreamMap().put(stream.get(0), stringBuilder1.toString());
                        StringBuilder stringBuilder2 = new StringBuilder("@Source(type = 'tcp',url='tcp://${" + listSiddhiApps.get(j).getAppName() + " source_ip}:{" + listSiddhiApps.get(j).getAppName() + " source_port}/" + stream.get(0) + "\'" + ", context=\'" + stream.get(0) + "\',@map(type='binary'))").append(definition);
                        listSiddhiApps.get(j).getInputStreamMap().put(stream.get(0), stringBuilder2.toString());

                    }


                }
            }
        }

        for (int i = 0; i < listSiddhiApps.size(); i++) {
            System.out.println(listSiddhiApps.get(i).toString());

        }


    }

    private String returnStream(String key, StrSiddhiApp strSiddhiApp) {

        if (siddhiAppRuntime.getStreamDefinitionMap().containsKey(key))
        {
            return siddhiAppRuntime.getStreamDefinitionMap().get(key).toString();

        } else if (siddhiAppRuntime.getTableDefinitionMap().containsKey(key))
        {
            return siddhiAppRuntime.getTableDefinitionMap().get(key).toString();

        } else if (strSiddhiApp.getOutputStreamMap().containsKey(key)) {

            return strSiddhiApp.getOutputStreamMap().get(key);

        } else {
            return null;
        }

    }
}