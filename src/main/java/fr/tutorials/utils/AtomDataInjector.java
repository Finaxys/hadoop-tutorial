package fr.tutorials.utils;

import java.util.Collection;
import java.util.List;

import v13.Day;
import v13.Order;
import v13.OrderBook;
import v13.PriceRecord;
import v13.agents.Agent;
import fr.tutorials.utils.hbase.AgentReferentialLine;

/**
 *
 */
public interface AtomDataInjector {
	
  public void setTimeStampBuilder(TimeStampBuilder tsb);

  public void createOutput() throws HadoopTutorialException;

  public void sendAgent(Agent a, Order o, PriceRecord pr) throws HadoopTutorialException;

  public void sendPriceRecord(PriceRecord pr, long bestAskPrice, long bestBidPrice) throws HadoopTutorialException;

  public void sendAgentReferential(List<AgentReferentialLine> referencial) throws HadoopTutorialException;

  public void sendOrder(Order o) throws HadoopTutorialException;

  public void sendTick(Day day, Collection<OrderBook> orderbooks) throws HadoopTutorialException;

  public void sendDay(int nbDays, Collection<OrderBook> orderbooks) throws HadoopTutorialException;

  public void sendExec(Order o) throws HadoopTutorialException;

  public void close() throws HadoopTutorialException;
}
