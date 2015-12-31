package fr.finaxys.tutorials.utils.spark.analysis;

import fr.finaxys.tutorials.utils.AtomAnalysis;
import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.hbase.AtomHBaseHelper;
import fr.finaxys.tutorials.utils.spark.batch.HBaseSparkRequester;
import fr.univlille1.atom.trace.OrderTrace;
import fr.univlille1.atom.trace.TraceType;

import java.util.*;
import java.util.logging.Level;

import org.apache.hadoop.conf.Configuration;

import com.sun.istack.NotNull;

/**
 * Created by finaxys on 12/11/15.
 */
public class SparkHBaseAnalysis extends AtomHBaseHelper implements AtomAnalysis  {
	
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(SparkHBaseAnalysis.class.getName());

    private String sparkTableName;
    
    
    
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
        HBaseSparkRequester requester = new HBaseSparkRequester(
        		this.configuration,
        		tableName.getNameAsString(),
        		columnFamily,
        		getSparkTableName());
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
        String request = "select  records.sender as Sender , records.obName as ObName , sum(records.quantity) as totalQty from records where records.trace='Order' and  records.timestamp <= "+maxStamp+" and records.timestamp >= "+minStamp+" GROUP BY records.obName , records.sender " ;
        HBaseSparkRequester requester = new HBaseSparkRequester(
        		this.configuration,
        		tableName.getNameAsString(),
        		columnFamily,
        		getSparkTableName());
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

	public String getSparkTableName() {
		return sparkTableName;
	}

	public void setSparkTableName(String sparkTableName) {
		this.sparkTableName = sparkTableName;
	}
}
