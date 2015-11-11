package fr.finaxys.tutorials.utils.hbase;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import fr.finaxys.tutorials.utils.AtomAnalysis;
import fr.univlille1.atom.trace.OrderTrace;
import fr.univlille1.atom.trace.TraceType;

public class HBaseAnalysis extends AtomHBaseHelper implements AtomAnalysis {
	
	private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
			.getLogger(HBaseAnalysis.class.getName());

	@Override
	public Map<TraceType, Integer> traceCount(Date date, List<TraceType> types) {
		Map<TraceType, Integer> ret = new HashMap<TraceType, Integer>();
//		try {
			Scan scan = mkScan();
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			long minStamp = cal.getTimeInMillis();
			cal.add(Calendar.DAY_OF_YEAR, 1);
			long maxStamp = cal.getTimeInMillis();
			LOGGER.log(Level.INFO, "traceCount Date : " +date + ", Min TimeStamp : "+ minStamp + ", Max TimeStamp : "+ maxStamp);
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
			ResultScanner results = scanTable(scan);
			for (Result r : results) {
				TraceType key = lookupType(r);
				Integer count = ret.get(key);
				if (count == null) count = new Integer(1);
				else count = new Integer(count.intValue() + 1);
				ret.put(key, count);
			}
//		} catch (IOException e) {
//			throw new HadoopTutorialException("IOException while trace counting", e);
//		}
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
