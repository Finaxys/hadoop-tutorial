package fr.tutorials.utils;

import v13.Day;
import v13.Order;
import v13.OrderBook;
import v13.PriceRecord;
import v13.agents.Agent;

import java.util.Collection;
import java.util.List;

import fr.tutorials.utils.hbase.AgentReferentialLine;

/**
 *
 */
public interface AtomDataInjector {

  public void createOutput() throws Exception;

  public void sendAgent(Agent a, Order o, PriceRecord pr);

  public void sendPriceRecord(PriceRecord pr, long bestAskPrice, long bestBidPrice);

  public void sendAgentReferential(List<AgentReferentialLine> referencial);

  public void sendOrder(Order o);

  public void sendTick(Day day, Collection<OrderBook> orderbooks);

  public void sendDay(int nbDays, Collection<OrderBook> orderbooks);

  public void sendExec(Order o);

  public void close();
}
