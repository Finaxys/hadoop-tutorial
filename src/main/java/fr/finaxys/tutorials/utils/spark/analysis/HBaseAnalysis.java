package fr.finaxys.tutorials.utils.spark.analysis;

import fr.finaxys.tutorials.utils.AtomAnalysis;
import fr.finaxys.tutorials.utils.hbase.AtomHBaseHelper;
import fr.finaxys.tutorials.utils.spark.batch.HBaseSparkRequester;
import fr.univlille1.atom.trace.OrderTrace;
import fr.univlille1.atom.trace.TraceType;

import java.util.*;
import java.util.logging.Level;

/**
 * Created by finaxys on 12/11/15.
 */
public class HBaseAnalysis extends AtomHBaseHelper implements AtomAnalysis  {


    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(HBaseAnalysis.class.getName());

    @Override
    public Map<TraceType, Integer> traceCount(Date date, List<TraceType> types) {
        //pre-request
        Map<TraceType, Integer> ret = new HashMap<TraceType, Integer>();
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        long minStamp = cal.getTimeInMillis();
        cal.add(Calendar.DAY_OF_YEAR, 1);
        long maxStamp = cal.getTimeInMillis();
        LOGGER.log(Level.INFO, "traceCount Date : " +date + ", Min TimeStamp : "+ minStamp + ", Max TimeStamp : "+ maxStamp);

        //request
        String request = "select records.trace as trace , count(*) as total from records where records.timestamp <= "+maxStamp+" and records.timestamp >= "+minStamp+" GROUP BY records.trace" ;
        HBaseSparkRequester requester = new HBaseSparkRequester(this.configuration);
        org.apache.spark.sql.Row[] result = requester.executeRequest(request);

        //post-request
        for(int i=0 ; i< result.length ; i++){
            ret.put(TraceType.valueOf(result[i].getString(0)),new Integer((int)result[i].getLong(1)));
        }
/*        // we can use it to eliminate nulls
        for(TraceType trace : TraceType.values()){
            if(! ret.containsKey(trace.toString())){
                ret.put(trace,new Integer(0));
            }
        }*/
        return ret;
    }



    @Override
    public Map<String, Integer> agentPosition(Date date) {

        Map<String, Integer> ret = new HashMap<String, Integer>();

   /*     Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        long minStamp = cal.getTimeInMillis();
        cal.add(Calendar.DAY_OF_YEAR, 1);
        long maxStamp = cal.getTimeInMillis();
        LOGGER.log(Level.INFO, "agentPosition Date : " +date + ", Min TimeStamp : "+ minStamp + ", Max TimeStamp : "+ maxStamp);

        AgentPosition ag = new AgentPosition();
        ag.setConf(configuration);
        try {
            String[] args = {Long.toString(minStamp),Long.toString(maxStamp),
                    this.tableName.getNameAsString(), Bytes.toString(this.columnFamily)};
            ag.run(args);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE,"Could not execute agent position", e);
        }

        try {
            Scan s = new Scan();
            s.addFamily(Bytes.toBytes(AgentPosition.AP_RESULT_CF));
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(AgentPosition.AP_RESULT_TABLE));
            ResultScanner results = table.getScanner(s);
            for (Result r : results) {
                String key = Bytes.toString(r.getRow());
                Integer l = Bytes.toInt(r.getValue(Bytes.toBytes(AgentPosition.AP_RESULT_CF), Bytes.toBytes(AgentPosition.AP_RESULT_QUAL)));
                ret.put(key, l);
            }
            table.close();
            connection.close();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE,"Could not parse result of agent position", e);
        };
*/
        return ret;
    }

    @Override
    public Map<String, Integer> agentOrderExecRate(Date date) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, OrderTrace> leftOrder(Date date, String agent) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, Map<String, Integer>> orderBookAvgOrderNotionalByDirection(
            Date date, String sector) {
        // TODO Auto-generated method stub
        return null;
    }


}
