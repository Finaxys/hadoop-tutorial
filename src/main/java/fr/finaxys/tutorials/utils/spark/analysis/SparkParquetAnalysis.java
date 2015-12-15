package fr.finaxys.tutorials.utils.spark.analysis;

import fr.finaxys.tutorials.utils.AtomAnalysis;
import fr.finaxys.tutorials.utils.spark.batch.ParquetSparkRequester;
import fr.univlille1.atom.trace.OrderTrace;
import fr.univlille1.atom.trace.TraceType;
import org.apache.hadoop.conf.Configuration;

import java.util.*;
import java.util.logging.Level;

/**
 * Created by finaxys on 12/15/15.
 */
public class SparkParquetAnalysis  implements AtomAnalysis {

    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(SparkParquetAnalysis.class.getName());
    private Configuration configuration ;

    public SparkParquetAnalysis(Configuration configuration){
        this.configuration = configuration ;
    }
    public SparkParquetAnalysis(){
    }

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
        String request = "select records.type as trace , count(*) as total from records where records.Timestamp <= "+maxStamp+" and records.Timestamp >= "+minStamp+" GROUP BY records.type" ;
        ParquetSparkRequester requester = new ParquetSparkRequester(this.configuration);
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
        //pre-request
        Map<String, Integer> ret = new HashMap<String, Integer>();
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        long minStamp = cal.getTimeInMillis();
        cal.add(Calendar.DAY_OF_YEAR, 1);
        long maxStamp = cal.getTimeInMillis();
        LOGGER.log(Level.INFO, "agentPosition Date : " +date + ", Min TimeStamp : "+ minStamp + ", Max TimeStamp : "+ maxStamp);
        //request
        //request
        String request = "select  order.Sender as Sender , order.ObName as ObName , sum(order.Quantity) as totalQty from records where records.Timestamp <= "+maxStamp+" and records.Timestamp >= "+minStamp+" GROUP BY order.ObName , order.Sender " ;
        ParquetSparkRequester requester = new ParquetSparkRequester(this.configuration);
        org.apache.spark.sql.Row[] result = requester.executeRequest(request);
        for(int i=0 ; i< result.length ; i++){
            ret.put(result[i].getString(0)+"-"+result[i].getString(1),new Integer((int)result[i].getLong(2)));
        }
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
