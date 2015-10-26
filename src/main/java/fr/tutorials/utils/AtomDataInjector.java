package fr.tutorials.utils;

import v13.Day;
import v13.Order;
import v13.OrderBook;
import v13.PriceRecord;
import v13.agents.Agent;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import fr.tutorials.utils.hbase.AgentReferentialLine;

/**
 *
 */
public interface AtomDataInjector {

  public void createOutput() throws Exception;

  public void sendAgent(Agent a, Order o, PriceRecord pr) throws IOException;

  public void sendPriceRecord(PriceRecord pr, long bestAskPrice, long bestBidPrice) throws IOException;

  public void sendAgentReferential(List<AgentReferentialLine> referencial);

  public void sendOrder(Order o) throws IOException;

  public void sendTick(Day day, Collection<OrderBook> orderbooks) throws IOException;

  public void sendDay(int nbDays, Collection<OrderBook> orderbooks) throws IOException;

  public void sendExec(Order o) throws IOException;

  public void close() throws IOException;
}
