package fr.finaxys.tutorials.utils.hbase;

import fr.finaxys.tutorials.utils.AtomAnalysis;
import fr.univlille1.atom.trace.OrderTrace;
import fr.univlille1.atom.trace.TraceType;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;

public class HBaseAnalysis extends AtomHBaseHelper implements AtomAnalysis {
	

	private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
			.getLogger(HBaseAnalysis.class.getName());

	@Override
	public Map<TraceType, Integer> traceCount(Date date, List<TraceType> types) {
		Map<TraceType, Integer> ret = new HashMap<TraceType, Integer>();
//		try {
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			long minStamp = cal.getTimeInMillis();
			cal.add(Calendar.DAY_OF_YEAR, 1);
			long maxStamp = cal.getTimeInMillis();
			LOGGER.log(Level.INFO, "traceCount Date : " +date + ", Min TimeStamp : "+ minStamp + ", Max TimeStamp : "+ maxStamp);
			
			Scan scan = mkScan();
			// @TODO is there better way ? Doesn't seems to work
			//scan.setTimeRange(minStamp, maxStamp);
			
			List<Filter> myList = new ArrayList<Filter>();
			// add a TraceType Filter
//			for (TraceType type : types) {
//			    Filter f = new SingleColumnValueFilter(columnFamily, Q_, 
//						  CompareOp.EQUAL,
//						  Bytes.toBytes(entry.getValue()));
//			    myList.add(f);
//			}
			Filter f1 = new SingleColumnValueFilter(columnFamily, Q_TIMESTAMP, 
					  CompareOp.GREATER_OR_EQUAL,
					  new LongComparator(minStamp));
			myList.add(f1);
			Filter f2 = new SingleColumnValueFilter(columnFamily, Q_TIMESTAMP, 
					  CompareOp.LESS,
					  new LongComparator(maxStamp));
			myList.add(f2);
			FilterList myFilterList =
					new FilterList(FilterList.Operator.MUST_PASS_ALL, myList);
			scan.setFilter(myFilterList);
			Table table = getTable();
			ResultScanner results = scanTable(table, scan);
			for (Result r : results) {
				TraceType key = lookupType(r.getRow());
				Integer count = ret.get(key);
				if (count == null) count = new Integer(1);
				else count = new Integer(count.intValue() + 1);
				ret.put(key, count);
			}
			returnTable(table);
//		} catch (IOException e) {
//			throw new HadoopTutorialException("IOException while trace counting", e);
//		}
		return ret;
	}
	
	@Override
	public Map<String, Integer> agentPosition(Date date) {
		
		Map<String, Integer> ret = new HashMap<String, Integer>();
		
		Calendar cal = Calendar.getInstance();
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
