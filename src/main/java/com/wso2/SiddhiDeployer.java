package com.wso2;


import org.apache.commons.collections.map.HashedMap;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.siddhi.query.api.execution.ExecutionElement;
import org.wso2.siddhi.query.api.execution.partition.Partition;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.handler.Filter;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class SiddhiDeployer {

    private static final Logger logger = Logger.getLogger(SiddhiDeployer.class);
    int[] queryContextEndIndex;
    private int[] queryContextStartIndex;
    private Map<String, StrSiddhiApp> distributiveMap;
    private SiddhiAppRuntime siddhiAppRuntime;
    Map<String, String> inmemoryMap;
    Map<String, Integer> partitionMap;


    public SiddhiDeployer() {
        this.distributiveMap = new LinkedHashMap<String, StrSiddhiApp>();
        this.inmemoryMap = new HashMap<String, String>();
        this.partitionMap = new HashedMap();
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

                // "@info(name = 'query1') @dist(parallel ='1',execGroup='group1')" +
                "from SmartHomeData " +
                "select houseId as houseId, max(value) as maxVal, min(value) as minVal, avg(value) as avgVal group by houseId " +
                "insert into UsageStream; " +

                "@info(name = 'query2') @dist(parallel ='2', execGroup='group2')" +
                "from UsageStream " +
                "select houseId, maxVal, minVal, avgVal " + "insert into OutputStream;" +

                "from UsageStream " +
                "select houseId, maxVal, minVal, avgVal " + "insert into OutputStream;";
        ;

        String siddhiAppString1 = "@App:name(\"SmartHomePlan\") " +
                "@Source(type = 'tcp', context='StockStream',@map(type='binary')) " +
                "define stream StockStream (symbol string, price float, volume long);" +

                "define stream CheckStockStream (symbol string, volume long);" +

                "define table StockTable (symbol string, price float, volume long);" +

                "@info(name = 'query1') @dist(execGroup='group1')" +
                "from StockStream " +
                "insert into StockTable ;" +

                "@info(name = 'query2') @dist(execGroup='group2')" + "from CheckStockStream[(StockTable.symbol==symbol) in StockTable]" + "insert into OutStream;";


        //(Defined) Window in two execGroups
        String siddhiAppString2 = "@App:name(\"SmartHomePlan\") " +
                "@Source(type = 'tcp', context='TempStream'," +
                "@map(type='binary')) " +
                "define stream TempStream(deviceID long, roomNo int, temp double);" +
                "define stream RegulatorStream(deviceID long, roomNo int, isOn bool);" +
                "define window TempWindow(deviceID long, roomNo int, temp double) time(1 min);" +

                "@info(name = 'query1') @dist(execGroup='group1')" +
                "from TempStream[temp > 30.0]" +
                "insert into TempWindow; " +

                "@info(name = 'query2')  @dist(execGroup='group1')" +
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
                "@info(name = 'query1') @dist(parallel ='2', execGroup='group1')\n " +
                "from every e1=TempStream, e2=TempStream[e1.temp + 1 < temp ]\n" +
                "select e1.temp as initialTemp, e2.temp as finalTemp\n" +
                "insert into AlertStream;";

        //Partition
        String siddhiAppString11 = "@App:name(\"SmartHomePlan\") " +
                "@Source(type = 'tcp', context='TempStream'," +
                "@map(type='binary')) " +
                "define stream TempStream(deviceID long, roomNo int, temp double); " +
                "@info(name = 'query1') @dist(parallel ='2', execGroup='group1')\n " +
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
                "@info(name = 'query2') @dist(parallel ='2', execGroup='group2') " +
                "from FiveMinTempWindow\n" +
                "select name, max(temp) as maxValue, roomNo\n" +
                "insert into MaxSensorReadingStream;";

        String siddiAppString14 = "@App:name(\"SmartHomePlan\") \n" +
                "@source(type='http', receiver.url='http://localhost:9055/endpoints/stockQuote', @map(type='xml')) " +
                "Define stream stockStream(symbol string, price float, quantity int, tier string);\n" +
                "\n" +
                "@Sink(type='email', @map(type='json'), username='wso2', address='test@wso2.com',password='****',host='smtp.gmail.com',subject='Event from SP',to='towso2@gmail.com')\n" +
                "Define stream takingOverStream(symbol string, overtakingSymbol string, avgPrice double);\n" +
                "\n" +
                "@source(type='http', receiver.url='http://localhost:9055/endpoints/trigger', @map(type='xml'))\n" +
                "Define stream companyTriggerStream(symbol string);\n" +
                "\n" +
                "@Store(type='rdbms', jdbc.url='jdbc:mysql://localhost:3306/cepDB',jdbc.driver.name='', username='root', password='****',field.length='symbol:254')\n" +
                "Define table filteredTable (symbol string, price float, quantity int, tier string);\n" +

                "@Store(type='rdbms', jdbc.url='jdbc:mysql://localhost:3306/spDB',jdbc.driver.name='', username='root', password='****',field.length='symbol:254')\n" +
                "Define table takingOverTable(symbol string, overtakingSymbol string, avgPrice double);\n" +
                "\n" +
                "@info(name = 'query1')@dist(parallel='2', execGroup='001')\n" +
                "From stockStream[price > 100]\n" +
                "Select *\n" +
                "Insert into filteredStockStream;\n" +
                "\n" +
                "@info(name='query2')@dist(parallel='2',execGroup='002')\n" +
                "Partition with (symbol of filteredStockStream)\n" +
                "begin\n" +
                "From filteredStockStream#window.time(5 min)\n" +
                "Select symbol, avg(price) as avgPrice, quantity\n" +
                "Insert into #avgPriceStream;\n" +
                "\n" +
                "From #avgPriceStream#window.time(5 min) as a right outer join companyTriggerStream#window.length(1)\n" +
                "On (companyTriggerStream.symbol == a.symbol)\n" +
                "Select a.symbol, a.avgPrice, a.quantity\n" +
                "Insert into triggeredAvgStream;\n" +
                "End;\n" +
                "\n" +
                "@info(name='query3')@dist(parallel='1', execGroup='003')\n" +
                "From  a1=triggeredAvgStream,  a2=triggeredAvgStream[a1.avgPrice<a2.avgPrice]\n" +
                "Select a1.symbol, a2.symbol as overtakingSymbol, a2.avgPrice \n" +
                "Insert into takingOverStream;\n" +
                "\n" +
                "@info(name='query4')@dist(parallel='4', execGroup='004')\n" +
                "From filteredStockStream\n" +
                "Select *\n" +
                "Insert into filteredTable;\n" +
                "\n" +
                "@info(name='query5')@dist(parallel='4', execGroup='004')\n" +
                "From takingOverStream\n" +
                "Select *\n" +
                "Insert into takingOverTable;\n" +
                "\n" +
                "@info(name='query6')@dist(parallel='3', execGroup='005')\n" +
                "Partition with (tier of filteredStockStream)\n" +
                "begin\n" +
                "From filteredStockStream#log(tier)\n" +
                "Select *\n" +
                "Insert into dumbstream;\n" +
                "End;\n";


        siddhiDeployer.DistributeSiddiApp(siddiAppString14);


    }


    //TODO:managing distribution to stream definitions not -this can be implemented by storing inmemory-table,window list common to all the queries
    //TODO:check if partition streams are assigned well
    //TODO:check for behavior
    //TODO:create Json file format
    //TODO:add highest partition parallel for json file format output streamID


    public void DistributeSiddiApp(String siddhiAppString) {

        SiddhiApp siddhiApp = SiddhiCompiler.parse(siddhiAppString);
        SiddhiManager siddhiManager = new SiddhiManager();

        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiAppString);


        StrSiddhiApp siddhiAppdist;
        String groupName;
        int parallel;


        for (ExecutionElement executionElement : siddhiApp.getExecutionElementList()) {

            parallel = 1;
            groupName = null;

            for (int i = 0; i < executionElement.getAnnotations().size(); i++) {
                if (executionElement.getAnnotations().get(i).getElement("execGroup") != null) {
                    groupName = executionElement.getAnnotations().get(i).getElement("execGroup");

                }

                if (executionElement.getAnnotations().get(i).getElement("parallel") != null) {
                    parallel = Integer.parseInt(executionElement.getAnnotations().get(i).getElement("parallel"));

                }
            }
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

            //if execution element is a query
            if (executionElement instanceof Query) {

                //set query
                queryContextStartIndex = ((Query) executionElement).getQueryContextStartIndex();
                queryContextEndIndex = ((Query) executionElement).getQueryContextEndIndex();
                siddhiAppdist.setQuery(ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString));

                distributiveMap.put(groupName, decomposeSiddhiApp((Query) executionElement, groupName, parallel, siddhiAppdist, siddhiAppString, siddhiApp));


            } else if (executionElement instanceof Partition) {


                //set query
                queryContextStartIndex = ((Partition) executionElement).getQueryContextStartIndex();
                queryContextEndIndex = ((Partition) executionElement).getQueryContextEndIndex();
                siddhiAppdist.setQuery(ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString));

                List<Query> partitionQueryList = ((Partition) executionElement).getQueryList();
                for (Query query : partitionQueryList) {
                    for (int k = 0; k < query.getAnnotations().size(); k++) {
                        if (query.getAnnotations().get(k).getElement("dist") != null) {
                            throw new SiddhiAppValidationException("Unsupported:@dist annotation in inside partition queries");
                        }
                    }
                    parallel = 1;


                    distributiveMap.put(groupName, decomposeSiddhiApp(query, groupName, parallel, siddhiAppdist, siddhiAppString, siddhiApp));
                }


            }


        }

        createJsonConfigurationFile();

    }


    private StrSiddhiApp decomposeSiddhiApp(Query executionElement, String groupName, int parallel, StrSiddhiApp siddhiAppdist, String siddhiAppString, SiddhiApp siddhiApp) {
        String[] inputStreamDefinition;
        String[] outputStreamDefinition;
        List<String> listInputStream;


        InputStream inputStream = (executionElement).getInputStream();
        if (parallel > 1) {
            //send to check for validity of the query type eg:join , window, pattern , sequence
            checkQueryType(inputStream);

        }

        String streamConsumptionStrategy = checkQueryStrategy(inputStream, executionElement);


        listInputStream = (executionElement).getInputStream().getAllStreamIds();
        for (int j = 0; j < listInputStream.size(); j++) {

            String inputStreamId = listInputStream.get(j);

            //not an inner Stream
            if (!inputStreamId.contains("#")) {
                inputStreamDefinition = returnStreamDefinition(inputStreamId, siddhiApp, siddhiAppString, parallel, groupName);
                siddhiAppdist.setInputStream(inputStreamId, inputStreamDefinition[0], inputStreamDefinition[1], streamConsumptionStrategy);
            }


        }

        String outputStreamId = executionElement.getOutputStream().getId();

        //not an inner Stream
        if (!outputStreamId.contains("#")) {
            outputStreamDefinition = returnStreamDefinition(outputStreamId, siddhiApp, siddhiAppString, parallel, groupName);
            siddhiAppdist.setOutputStream(outputStreamId, outputStreamDefinition[0], outputStreamDefinition[1]);

        }


        return siddhiAppdist;


    }


    private String[] returnStreamDefinition(String streamId, SiddhiApp siddhiApp, String siddhiAppString, int parallel, String groupName) {

        String[] streamDefinition = new String[2];

        if (siddhiApp.getStreamDefinitionMap().containsKey(streamId)) {


            queryContextStartIndex = siddhiApp.getStreamDefinitionMap().get(streamId).getQueryContextStartIndex();
            queryContextEndIndex = siddhiApp.getStreamDefinitionMap().get(streamId).getQueryContextEndIndex();
            streamDefinition[0] = ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString);
            streamDefinition[1] = "Stream";


        } else if (siddhiApp.getTableDefinitionMap().containsKey(streamId)) {


            AbstractDefinition tableDefinition = siddhiApp.getTableDefinitionMap().get(streamId);
            streamDefinition[1] = "InMemoryTable";

            for (int k = 0; k < tableDefinition.getAnnotations().size(); k++) {
                if (tableDefinition.getAnnotations().get(k).getName().equals("Store")) {
                    streamDefinition[1] = "Table";
                }
            }
            //need to check In-Memory or other
            if (parallel != 1 && streamDefinition[1].equals("InMemoryTable")) {

                throw new SiddhiAppValidationException("In-Memory Tables can not have parallel >1");
            }

            queryContextStartIndex = siddhiApp.getTableDefinitionMap().get(streamId).getQueryContextStartIndex();
            queryContextEndIndex = siddhiApp.getTableDefinitionMap().get(streamId).getQueryContextEndIndex();
            streamDefinition[0] = ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString);

            if (streamDefinition[1].equals("InMemoryTable") && inmemoryMap.containsKey(streamId)) {
                if (!inmemoryMap.get(streamId).equals(groupName)) {
                    throw new SiddhiAppValidationException("Unsupported:Event Table " + streamId +
                            " In-Memory Table used in two execGroups: execGroup " + groupName + " && " + inmemoryMap.get(streamId));
                }
            } else {
                inmemoryMap.put(streamId, groupName);
            }

        } else if (siddhiApp.getWindowDefinitionMap().containsKey(streamId)) {

            if (parallel != 1) {
                throw new SiddhiAppValidationException("(Defined) Window can not have parallel >1");
            }

            queryContextStartIndex = siddhiApp.getWindowDefinitionMap().get(streamId).getQueryContextStartIndex();
            queryContextEndIndex = siddhiApp.getWindowDefinitionMap().get(streamId).getQueryContextEndIndex();
            streamDefinition[0] = ExceptionUtil.getContext(queryContextStartIndex, queryContextEndIndex, siddhiAppString);
            streamDefinition[1] = "Window";

            if (inmemoryMap.containsKey(streamId)) {
                if (!inmemoryMap.get(streamId).equals(groupName)) {
                    throw new SiddhiAppValidationException("Unsupported:(Defined) Window " + streamId +
                            " In-Memory window used in two execGroups: execGroup " + groupName + " && " + inmemoryMap.get(streamId));
                }
            } else {
                inmemoryMap.put(streamId, groupName);
            }


            //if stream definition is an inferred definition
        } else if (streamDefinition[0] == null) {

            if (siddhiAppRuntime.getStreamDefinitionMap().containsKey(streamId)) {

                streamDefinition[0] = siddhiAppRuntime.getStreamDefinitionMap().get(streamId).toString();
                streamDefinition[1] = "Stream";

            } else if (siddhiAppRuntime.getTableDefinitionMap().containsKey(streamId)) {

                if (parallel != 1) {
                    throw new SiddhiAppValidationException("(In-Memory Tables can not have parallel >1");
                }

                streamDefinition[0] = siddhiAppRuntime.getTableDefinitionMap().get(streamId).toString();
                streamDefinition[1] = "InMemoryTable";

            }

        }

        return streamDefinition;

    }

    private void createJsonConfigurationFile() {

        List<StrSiddhiApp> listSiddhiApps = new ArrayList<StrSiddhiApp>(distributiveMap.values());

        for (int i = 0; i < listSiddhiApps.size(); i++) {
            System.out.println(listSiddhiApps.get(i).toString());
        }

        CreateJson createJson = new CreateJson();
        try {
            createJson.writeConfiguration(listSiddhiApps, "/home/piyumi/deployment.json");
        } catch (IOException e) {
            logger.error(e);
        }
    }

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

    private String checkQueryStrategy(InputStream inputStream, ExecutionElement executionElement, int parallel, String streamId) {


        //TODO:if parallel=1 then -->strategy = A
        //TODO:if parallel>1 then -->if partitionedStream -->strategy=F
        //TODO:if parallel >1 then -->if not partitioned Stream --->strategy = A
        //TODO:if parallel >1 then -->if Filter and outside partition  not partitioned stream--->strategy = R
        //TODO:if parallel >1 then -->if Filter and outside partition and partioned stream ----> unsupported


        if (parallel > 1) {
            if (executionElement instanceof Partition) {
                if (((Partition) executionElement).getPartitionTypeMap().containsKey(streamId)) {

                    if (!partitionMap.containsKey(streamId)) {
                        partitionMap.put(streamId, parallel);

                    } else {
                        if (partitionMap.get(streamId) < parallel) {
                            partitionMap.put(streamId, parallel);
                        }

                    }
                    //field grouping
                    return "F";
                } else {
                    //inside a partition but not a partitioned stream
                    return "A";
                }

            }


        } else {
            //all events to all subscribers
            return "A";

        }
        

        if (inputStream instanceof SingleInputStream) {
            List<StreamHandler> streamHandlers = ((SingleInputStream) inputStream).getStreamHandlers();

            for (int i = 0; i < streamHandlers.size(); i++) {
                if (streamHandlers.get(i) instanceof Filter) {
                    return "R";//round robin strategy
                }
            }
        }
        return "A";
    }

}



