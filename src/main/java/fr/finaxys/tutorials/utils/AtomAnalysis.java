package fr.finaxys.tutorials.utils;

import java.util.Date;
import java.util.List;
import java.util.Map;

import fr.univlille1.atom.trace.OrderTrace;
import fr.univlille1.atom.trace.TraceType;

public interface AtomAnalysis {
	
	/**
	 * Example: simple count with one criteria
	 * 
	 * Return the number of trace by type of a specific trading date
	 * @param date the trading date to analyse
	 * @return k:v TraceType / Count
	 */
	public Map<TraceType, Integer> traceCount(Date date, List<TraceType> types);
	
	/**
	 * Example: agent position => filter and count
	 * 
	 * Return the rate of order/exec for a specific trading day for each agent .
	 * @param date the trading date to analyse
	 * @return k:v AgentName / Order-Exec Rate
	 */
	public Map<String, Integer> agentPosition(Date date);
	
	/**
	 * Example: ratio with join => on two distinct dataset: Exec and Order
	 * 
	 * Return the rate of order/exec for a specific trading day for each agent .
	 * @param date the trading date to analyse
	 * @return k:v AgentName / Order-Exec Rate
	 */
	public Map<String, Integer> agentOrderExecRate(Date date);
	
	/**
	 * Example: MR/Hive join on two dataset. HBase: flag by coprocessor the order that has been fill. 
	 * 
	 * Return the trace that has not been totally fill for a specif agent on a specific date
	 * @param date the date to analyse
	 * @param agent the agent we are looking for non fill order
	 * @return k:v trace type / Count
	 */
	public Map<String, OrderTrace> leftOrder(Date date, String agent);
	
	/**
	 * Example: Cleansing example, Sector is not part of order information. Data must be enriched or join.
	 * 
	 * Return the average order notional by order book on a specific sector for a specific trading day by direction (Buy/Sell).
	 * @param date the trading date to analyse
	 * @param sector the sector of the orderbook
	 * @return k:<k:v> OrderBook Name / Buy/Sell / Average Order Notional
	 */
	public Map<String, Map<String, Integer>> orderBookAvgOrderNotionalByDirection(Date date, String sector);

}
