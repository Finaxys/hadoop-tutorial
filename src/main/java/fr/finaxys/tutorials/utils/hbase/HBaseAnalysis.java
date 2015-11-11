package fr.finaxys.tutorials.utils.hbase;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.htrace.Trace;

import fr.finaxys.tutorials.utils.AtomAnalysis;
import fr.univlille1.atom.trace.OrderTrace;
import fr.univlille1.atom.trace.TraceType;

public class HBaseAnalysis extends AtomHBaseHelper implements AtomAnalysis {

	@Override
	public Map<TraceType, Integer> traceCount(Date date) {
		Map<TraceType, Integer> ret = new HashMap<TraceType, Integer>();
		Scan scan = mkScan();
		ResultScanner results = scanTable(scan);
		for (Result r : results) {
			TraceType key = lookupType(r);
			Integer count = ret.get(key);
			if (count == null) count = new Integer(1);
			else count = new Integer(count.intValue() + 1);
			ret.put(key, count);
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
