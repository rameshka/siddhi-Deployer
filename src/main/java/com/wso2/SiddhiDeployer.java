package com.wso2;


import org.apache.log4j.Logger;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.api.execution.ExecutionElement;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.util.ExceptionUtil;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;

import java.io.IOException;
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


    private static final Logger logger = Logger.getLogger(SiddhiDeployer.class);


    public SiddhiDeployer() {
        this.distributiveMap = new LinkedHashMap<String, StrSiddhiApp>();
    }

    public static void main(String[] args) {


        SiddhiDeployer siddhiDeployer = new SiddhiDeployer();


        String siddhiAppString = "@App:name(\"SmartHomePlan\") \n" +
                "@Source(type = 'tcp', context='SmartHomeData'," +
                "@map(type='binary')) " +
                "define stream SmartHomeData (id string, value float, property bool, plugId int, householdId int, houseId int, currentTime string); " +
               /* "@Sink(type='tcp', url='tcp://wso2-ThinkPad-T530:{}/OutputStream',context='OutputStream', port='9992'," +
                "@map(type='binary')) " +*/
                "define stream OutputStream (houseId int, maxVal float, minVal float, avgVal double); " +
                "@info(name = 'query1') @dist(parallel ='1',execGroup='group1')" +
                "from SmartHomeData " +
                "select houseId as houseId, max(value) as maxVal, min(value) as minVal, avg(value) as avgVal group by houseId " +
                "insert into UsageStream; " +
                "@info(name = 'query2') @dist(parallel ='2', execGroup='group2')" +

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

        try {
            siddhiDeployer.connectApplications();
        } catch (IOException e) {
            logger.error("Distributed execution plan creation failure", e);
        }


    }
    //TODO:if its a window parallelism in not allowed


    public void DistributeSiddiApp(String siddhiAppString) {

        StrSiddhiApp siddhiAppdist;
        List<String> listInputStream;
        SiddhiApp siddhiApp = SiddhiCompiler.parse(siddhiAppString);


        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppString);

        String groupName = null;
        String parallel = "1";


        for (ExecutionElement executionElement : siddhiApp.getExecutionElementList()) {


            for (int i = 0; i < executionElement.getAnnotations().size(); i++) {
                if (executionElement.getAnnotations().get(i).getElement("execGroup") != null) {
                    groupName = executionElement.getAnnotations().get(i).getElement("execGroup");

                }
                if (executionElement.getAnnotations().get(i).getElement("parallel") != null) {
                    parallel = executionElement.getAnnotations().get(i).getElement("parallel");

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

                if (outputStreamDefinition == null) {
                    outputStreamDefinition = returnInferredStream(outputStreamId);
                }
                siddhiAppdist.setOutputStream(outputStreamId, outputStreamDefinition);

                //query taken
                queryContextStartIndex = ((Query) executionElement).getQueryContextStartIndex();
                queryContextEndIndex = ((Query) executionElement).getQueryContextEndIndex();
                String strQuery = ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString);


                siddhiAppdist.setQuery(strQuery);

                siddhiAppdist.setParallel(parallel);

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


    private void connectApplications() throws IOException

    {
        List<String> stream;
        List<StrSiddhiApp> listSiddhiApps = new ArrayList<StrSiddhiApp>(distributiveMap.values());
        int parallel;
        String definition;

        StringBuilder stringBuilder1;

        for (int i = 0; i < listSiddhiApps.size(); i++) {

            //TODO:check if outputStream can be only 1
            stream = new ArrayList<String>(listSiddhiApps.get(i).getOutputStreamMap().keySet());

            for (int j = i + 1; j < listSiddhiApps.size(); j++) {

                if (listSiddhiApps.get(j).getInputStreamMap().containsKey(stream.get(0))) {

                     definition = listSiddhiApps.get(i).getOutputStreamMap().get(stream.get(0));
                    parallel = Integer.parseInt(listSiddhiApps.get(j).getParallel());

                    if (parallel > 1)
                    {
                        stringBuilder1 = new StringBuilder("@sink(type='tcp',  context=\'"+stream.get(0)+"\', @map(type='binary'), " +
                                "@distribution(strategy='roundRobin'");

                        for (int k = 0; k < parallel; k++) {
                            stringBuilder1.append(",@destination(url='tcp://${" + listSiddhiApps.get(i).getAppName() + " sink_ip}:{" + listSiddhiApps.get(i).getAppName() + " sink_port" ).append(k+1).append("}/"+stream.get(0)+"\')");
                        }


                        stringBuilder1.append("))" + definition);

                    } else
                    {
                        stringBuilder1 = new StringBuilder("@Sink(type = 'tcp',url='tcp://${" + listSiddhiApps.get(i).getAppName() + " sink_ip}:{" + listSiddhiApps.get(i).getAppName() + " sink_port1}/" + stream.get(0) + "\'" + ", context=\'" + stream.get(0) + "\',@map(type='binary'))").append(definition);
                    }


                    listSiddhiApps.get(i).getOutputStreamMap().put(stream.get(0), stringBuilder1.toString());
                    StringBuilder stringBuilder2 = new StringBuilder("@Source(type = 'tcp',url='tcp://${" + listSiddhiApps.get(j).getAppName() + " source_ip}:{" + listSiddhiApps.get(j).getAppName() + " source_port}/" + stream.get(0) + "\'" + ", context=\'" + stream.get(0) + "\',@map(type='binary'))").append(definition);
                    listSiddhiApps.get(j).getInputStreamMap().put(stream.get(0), stringBuilder2.toString());

                }
            }
        }

        //adding sink for the output stream of the final query to support testing
        //unless user has to give the sink
        //unless it will be a inmemory stream
        definition = listSiddhiApps.get(listSiddhiApps.size()-1).getOutputStreamMap().get("OutputStream");
        stringBuilder1 = new StringBuilder("@Sink(type = 'tcp',url='tcp://${" + listSiddhiApps.get(listSiddhiApps.size()-1).getAppName() + " sink_ip}:{" + listSiddhiApps.get(listSiddhiApps.size()-1).getAppName() + " sink_port1}/" + "OutputStream" + "\'" + ", context=\'" +  "OutputStream"+ "\',@map(type='binary'))").append(definition);
        listSiddhiApps.get(listSiddhiApps.size()-1).getOutputStreamMap().put("OutputStream",stringBuilder1.toString());



        for (int i = 0; i < listSiddhiApps.size(); i++) {
            System.out.println(listSiddhiApps.get(i).toString());

        }


        //creating JSON configuration file
        CreateJson createJson = new CreateJson();
        createJson.writeConfiguration(listSiddhiApps, "/home/piyumi/deployment.json");


    }

    private String returnInferredStream(String key) {

        if (siddhiAppRuntime.getStreamDefinitionMap().containsKey(key)) {
            return siddhiAppRuntime.getStreamDefinitionMap().get(key).toString();

        } else if (siddhiAppRuntime.getTableDefinitionMap().containsKey(key)) {
            return siddhiAppRuntime.getTableDefinitionMap().get(key).toString();

        } else {
            return null;
        }

    }
}