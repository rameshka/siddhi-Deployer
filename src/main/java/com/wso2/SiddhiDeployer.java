package com.wso2;


import org.apache.log4j.Logger;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.siddhi.query.api.execution.ExecutionElement;
import org.wso2.siddhi.query.api.execution.partition.Partition;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.handler.StreamHandler;
import org.wso2.siddhi.query.api.execution.query.input.handler.Window;
import org.wso2.siddhi.query.api.execution.query.input.stream.InputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;
import org.wso2.siddhi.query.api.util.ExceptionUtil;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class SiddhiDeployer {

    private static final Logger logger = Logger.getLogger(SiddhiDeployer.class);
    int[] queryContextEndIndex;
    private int[] queryContextStartIndex;
    private Map<String, StrSiddhiApp> distributiveMap;
    private SiddhiAppRuntime siddhiAppRuntime;


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


        String siddhiAppString2 = "@App:name(\"SmartHomePlan\") " +
                "@Source(type = 'tcp', context='TempStream'," +
                "@map(type='binary')) " +
                "define stream TempStream(deviceID long, roomNo int, temp double);" +
                "define stream RegulatorStream(deviceID long, roomNo int, isOn bool);" +
                "define window TempWindow(deviceID long, roomNo int, temp double) time(1 min);" +
                "@info(name = 'query1') @dist(execGroup='group1')" +
                "from TempStream[temp > 30.0]" +
                "insert into TempWindow; " +
                "@info(name = 'query2') @dist(execGroup='group2',parallel='2')" +
                "from TempWindow " +
                "join RegulatorStream[isOn == false]#window.length(1) as R " +
                "on TempWindow.roomNo == R.roomNo" +
                " select TempWindow.roomNo, R.deviceID, 'start' as action " +
                "insert into RegulatorActionStream;";


        String siddhiAppString3 = "@App:name(\"SmartHomePlan\") " +
                "@Source(type = 'tcp', context='TempStream'," +
                "@map(type='binary')) " +
                "define stream TempStream(deviceID long, roomNo int, temp double);" +
                "define window TempWindow(deviceID long, roomNo int, temp double) time(1 min);" +

                "@info(name = 'query1') @dist(execGroup='group1',parallel ='2')" +
                "from TempStream[temp > 30.0]" +
                "insert into TempWindow;" +
                "@info(name = 'query2') @dist(execGroup='group2',parallel='2')" +
                "from TempStream#window.time(5 sec)" +
                "output snapshot every 1 sec" +
                " insert into SnapshotTempStream;";

        String siddhiAppString4 = "@App:name(\"SmartHomePlan\") " +
                "@Source(type = 'tcp', context='TempStream'," +
                "@map(type='binary')) " +
                "define stream TempStream(deviceID long, roomNo int, temp double);" +
                "define window TempWindow(deviceID long, roomNo int, temp double) time(1 min);" +

                "@info(name = 'query1') @dist(execGroup='group1',parallel ='2')" +
                "from TempStream[temp > 30.0]" +
                "insert into TempWindow;" +
                "@info(name = 'query2') @dist(execGroup='group2',parallel='2')" +
                "from TempStream [(roomNo >= 100 and roomNo < 110) and temp > 40 ]\n" +
                "select roomNo, temp " +
                "insert into HighTempStream;";

        //BasicSingleInputStream
        String siddhiAppString5 = "@App:name(\"SmartHomePlan\") " +
                "@Source(type = 'tcp', context='TempStream'," +
                "@map(type='binary')) " +
                "define stream TempStream(deviceID long, roomNo int, temp double); " +
                "@info(name = 'query1') @dist(execGroup='group1',parallel ='2') " +
                "from TempStream " +
                "select roomNo, temp " +
                "insert into RoomTempStream;";

        //BasicSingle and Filter
        String siddhiAppString6 = "@App:name(\"SmartHomePlan\") " +
                "@Source(type = 'tcp', context='TempStream'," +
                "@map(type='binary')) " +
                "define stream TempStream(deviceID long, roomNo int, temp double); " +
                "@info(name = 'query2') @dist(parallel ='2', execGroup='group2')" +
                "from TempStream [(roomNo >= 100 and roomNo < 110) and temp > 40 ]\n" +
                "select roomNo, temp\n" +
                "insert into HighTempStream;";

        //SingleInputStream and Window
        String siddhiAppString7 = "@App:name(\"SmartHomePlan\") " +
                "@Source(type = 'tcp', context='TempStream'," +
                "@map(type='binary')) " +
                "define stream TempStream(deviceID long, roomNo int, temp double); " +
                "from TempStream#window.time(1 min)\n" +
                "select *\n" +
                "insert expired events into DelayedTempStream";

        //JoinInputStream
        String siddhiAppString8 = "@App:name(\"SmartHomePlan\") " +
                "@Source(type = 'tcp', context='TempStream'," +
                "@map(type='binary')) " +
                "define stream TempStream(deviceID long, roomNo int, temp double); " +
                "define stream RegulatorStream(deviceID long, roomNo int, isOn bool);\n" +
                " \n" +
                "from TempStream[temp > 30.0]#window.time(1 min) as T\n" +
                "  join RegulatorStream[isOn == false]#window.length(1) as R\n" +
                "  on T.roomNo == R.roomNo\n" +
                "select T.roomNo, R.deviceID, 'start' as action\n" +
                "insert into RegulatorActionStream;";

        //StateInputStream type Pattern
        String siddhiAppString9 = "@App:name(\"SmartHomePlan\") " +
                "@Source(type = 'tcp', context='TempStream'," +
                "@map(type='binary')) " +
                "define stream TempStream(deviceID long, roomNo int, temp double); " +
                "from every( e1=TempStream ) -> e2=TempStream[e1.roomNo==roomNo and (e1.temp + 5) <= temp ]\n" +
                "    within 10 min\n" +
                "select e1.roomNo, e1.temp as initialTemp, e2.temp as finalTemp\n" +
                "insert into AlertStream";

        //StateInputStream type Sequence
        String siddhiAppString10 = "@App:name(\"SmartHomePlan\") " +
                "@Source(type = 'tcp', context='TempStream'," +
                "@map(type='binary')) " +
                "define stream TempStream(deviceID long, roomNo int, temp double); " +
                "from every e1=TempStream, e2=TempStream[e1.temp + 1 < temp ]\n" +
                "select e1.temp as initialTemp, e2.temp as finalTemp\n" +
                "insert into AlertStream;";

        //Partition
        String siddhiAppString11 = "@App:name(\"SmartHomePlan\") " +
                "@Source(type = 'tcp', context='TempStream'," +
                "@map(type='binary')) " +
                "define stream TempStream(deviceID long, roomNo int, temp double); " +
                "partition with ( deviceID of TempStream )\n" +
                "begin\n" +
                "    from TempStream#window.length(10)\n" +
                "    select roomNo, deviceID, max(temp) as maxTemp\n" +
                "    insert into DeviceTempStream\n" +
                "end;";

        String siddhiAppString12 = "@App:name(\"SmartHomePlan\") " +
                "@Source(type = 'tcp', context='StockStream'," +
                "@map(type='binary')) " +
                "define stream StockStream (symbol string, price float, volume double);\n" +
                "define stream CheckStockStream (symbol string, volume long);" +
                "@PrimaryKey(\"symbol\")\n" +
                "@Store(type=\"rdbms\", jdbc.url=\"jdbc:mysql://localhost:3306/cepDB\",jdbc.driver.name=\"\", username=\"root\", password=\"ABPRameshka0508\",field.length=\"symbol:254\")\n" +
                "define table StockTable (symbol string, price float, volume double);\n" +
                "@info(name = 'query1') @dist(parallel ='2', execGroup='group1')\n " +
                "from StockStream\n" +
                "insert into StockTable ;\n" +
                "  \n" +
                "@info(name = 'query2')\n" +
                "from CheckStockStream[(StockTable.symbol==symbol) in StockTable]\n" +
                "insert into OutStream;";

        String siddhiAppString13 = "@App:name(\"SmartHomePlan\") \n" +
                "@dist(parallel ='2', execGroup='group2') " +
                "define window FiveMinTempWindow (roomNo int, temp double,name string) timeBatch(1 second);\n" +
                "@info(name = 'query2') @dist(parallel ='1', execGroup='group2') " +
                "from FiveMinTempWindow\n" +
                "select name, max(temp) as maxValue, roomNo\n" +
                "insert into MaxSensorReadingStream;";

        siddhiDeployer.DistributeSiddiApp(siddhiAppString11);


   /*     try {
            siddhiDeployer.connectApplications();
        } catch (IOException e) {
            logger.error("Distributed execution plan creation failure", e);
        }*/


    }

    //TODO:managing distribution to stream definitions not implemented

    public void DistributeSiddiApp(String siddhiAppString) {

        StrSiddhiApp siddhiAppdist;
        List<String> listInputStream;
        SiddhiApp siddhiApp = SiddhiCompiler.parse(siddhiAppString);


        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppString);


        String groupName = null;
        int parallel = 1;
        String[] inputStreamDefinition;
        String[] outputStreamDefinition;


        for (ExecutionElement executionElement : siddhiApp.getExecutionElementList()) {


            for (int i = 0; i < executionElement.getAnnotations().size(); i++) {
                if (executionElement.getAnnotations().get(i).getElement("execGroup") != null) {
                    groupName = executionElement.getAnnotations().get(i).getElement("execGroup");

                }
                if (executionElement.getAnnotations().get(i).getElement("parallel") != null) {
                    parallel = Integer.parseInt(executionElement.getAnnotations().get(i).getElement("parallel"));

                }
            }


            //if execution element is a query
            if (executionElement instanceof Query) {


                if (groupName != null && !distributiveMap.containsKey(groupName)) {

                    siddhiAppdist = new StrSiddhiApp();
                    siddhiAppdist.setAppName(groupName);
                    siddhiAppdist.setParallel(Integer.toString(parallel));

                } else if (distributiveMap.containsKey(groupName)) {
                    siddhiAppdist = distributiveMap.get(groupName);

                    //Same execution group given  with different parallel numbers
                    if (!siddhiAppdist.getParallel().equals(Integer.toString(parallel))) {

                        throw new SiddhiAppValidationException("execGroup =" + "\'" + groupName + "\' not assigned a unique @dist(parallel)");
                    }
                } else

                {
                    //will work if execGroup is not mentioned-those will go to a single app
                    siddhiAppdist = new StrSiddhiApp();


                    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                    siddhiAppdist.setAppName(Long.toString(timestamp.getTime()));
                    siddhiAppdist.setParallel(Integer.toString(parallel));
                }


               // if (parallel > 1) {
                    //send to check for validity of the query type eg:join , window, pattern , sequence
                    InputStream inputStream = ((Query) executionElement).getInputStream();
                    checkQueryType(inputStream);

//                }

                listInputStream = ((Query) executionElement).getInputStream().getAllStreamIds();

                for (int j = 0; j < listInputStream.size(); j++) {
                    String inputStreamId = listInputStream.get(j);
                    inputStreamDefinition = returnStreamDefinition(inputStreamId, siddhiApp, siddhiAppString, parallel);


                    siddhiAppdist.setInputStream(inputStreamId, inputStreamDefinition[0],inputStreamDefinition[1]);

                }

                String outputStreamId = ((Query) executionElement).getOutputStream().getId();
                outputStreamDefinition = returnStreamDefinition(outputStreamId, siddhiApp, siddhiAppString, parallel);

                siddhiAppdist.setOutputStream(outputStreamId, outputStreamDefinition[0],outputStreamDefinition[1]);

                //query taken
                queryContextStartIndex = ((Query) executionElement).getQueryContextStartIndex();
                queryContextEndIndex = ((Query) executionElement).getQueryContextEndIndex();
                String strQuery = ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString);


                siddhiAppdist.setQuery(strQuery);


                distributiveMap.put(groupName, siddhiAppdist);


            } else if (executionElement instanceof Partition) {
                List<Query> inputStream = ((Partition) executionElement).getQueryList();
                checkQueryType(inputStream.get(0).getInputStream());

            }


        }

    }


    private String[] returnStreamDefinition(String streamId, SiddhiApp siddhiApp, String siddhiAppString, int parallel) {

        String[] streamDefinition = null;

        if (siddhiApp.getStreamDefinitionMap().containsKey(streamId)) {

            queryContextStartIndex = siddhiApp.getStreamDefinitionMap().get(streamId).getQueryContextStartIndex();
            queryContextEndIndex = siddhiApp.getStreamDefinitionMap().get(streamId).getQueryContextEndIndex();
            streamDefinition[0] = ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString);
            streamDefinition[1] ="Stream";


        } else if (siddhiApp.getTableDefinitionMap().containsKey(streamId)) {

            AbstractDefinition tableDefinition = siddhiApp.getTableDefinitionMap().get(streamId);
            streamDefinition[1] ="InMemoryTable";

            for (int k = 0; k < tableDefinition.getAnnotations().size(); k++) {
                if (tableDefinition.getAnnotations().get(k).getElement("Store") != null) {
                    streamDefinition[1]="Table";
                }
            }
            //need to check In-Memory or other
            if (parallel != 1) {

                throw new SiddhiAppValidationException("In-Memory Tables can not have parallel >1");

            }
            queryContextStartIndex = siddhiApp.getTableDefinitionMap().get(streamId).getQueryContextStartIndex();
            queryContextEndIndex = siddhiApp.getTableDefinitionMap().get(streamId).getQueryContextEndIndex();
            streamDefinition[0] = ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString);

        } else if (siddhiApp.getWindowDefinitionMap().containsKey(streamId)) {

            if (parallel != 1) {
                throw new SiddhiAppValidationException("(Defined) Window can not have parallel >1");
            }

            queryContextStartIndex = siddhiApp.getWindowDefinitionMap().get(streamId).getQueryContextStartIndex();
            queryContextEndIndex = siddhiApp.getWindowDefinitionMap().get(streamId).getQueryContextEndIndex();
            streamDefinition[0] = ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString);
            streamDefinition[1] = "Window";


            //if stream definition is an inferred definition
        } else if (streamDefinition == null) {

            if (siddhiAppRuntime.getStreamDefinitionMap().containsKey(streamId)) {

                streamDefinition[0] = siddhiAppRuntime.getStreamDefinitionMap().get(streamId).toString();
                streamDefinition[1] = "Window";

            } else if (siddhiAppRuntime.getTableDefinitionMap().containsKey(streamId)) {
                streamDefinition[0] = siddhiAppRuntime.getTableDefinitionMap().get(streamId).toString();
                streamDefinition[1] = "Table";

            }

        }

        return streamDefinition;

    }


  /*  private void connectApplications() throws IOException

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

                    if (parallel > 1) {
                        stringBuilder1 = new StringBuilder("@sink(type='tcp',  context=\'" + stream.get(0) + "\', @map(type='binary'), " +
                                "@distribution(strategy='roundRobin'");

                        for (int k = 0; k < parallel; k++) {
                            stringBuilder1.append(",@destination(url='tcp://${" + listSiddhiApps.get(i).getAppName() + " sink_ip}:{" + listSiddhiApps.get(i).getAppName() + " sink_port").append(k + 1).append("}/" + stream.get(0) + "\')");
                        }


                        stringBuilder1.append("))" + definition);

                    } else {
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
        definition = listSiddhiApps.get(listSiddhiApps.size() - 1).getOutputStreamMap().get("OutputStream");
        stringBuilder1 = new StringBuilder("@Sink(type = 'tcp',url='tcp://${" + listSiddhiApps.get(listSiddhiApps.size() - 1).getAppName() + " sink_ip}:{" + listSiddhiApps.get(listSiddhiApps.size() - 1).getAppName() + " sink_port1}/" + "OutputStream" + "\'" + ", context=\'" + "OutputStream" + "\',@map(type='binary'))").append(definition);
        listSiddhiApps.get(listSiddhiApps.size() - 1).getOutputStreamMap().put("OutputStream", stringBuilder1.toString());


        for (int i = 0; i < listSiddhiApps.size(); i++) {
            System.out.println(listSiddhiApps.get(i).toString());

        }


        //creating JSON configuration file
        CreateJson createJson = new CreateJson();
        createJson.writeConfiguration(listSiddhiApps, "/home/piyumi/deployment.json");


    }*/

    private void checkQueryType(InputStream inputStream) {

        if (inputStream instanceof JoinInputStream) {
            throw new SiddhiAppValidationException("Join queries can not have parallel greater than 1  ");

        } else if (inputStream instanceof StateInputStream) {

            String type = ((StateInputStream) inputStream).getStateType().name();
            throw new SiddhiAppValidationException(type + " queries can not have parallel greater than 1  ");

        } else if (inputStream instanceof SingleInputStream) {
            List<StreamHandler> streamHandlers = ((SingleInputStream) inputStream).getStreamHandlers();

            for (int i = 0; i < streamHandlers.size(); i++) {
                if (streamHandlers.get(i) instanceof Window) {
                    throw new SiddhiAppValidationException("Window queries can not have parallel greater than 1  ");
                }
            }


        }

    }
}