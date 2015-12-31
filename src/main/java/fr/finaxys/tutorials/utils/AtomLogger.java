package fr.finaxys.tutorials.utils;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;

import v13.Day;
import v13.Logger;
import v13.Order;
import v13.OrderBook;
import v13.PriceRecord;
import v13.agents.Agent;

import com.sun.istack.NotNull;

public class AtomLogger extends Logger {
	
  private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(AtomLogger.class.getName());

  private AtomDataInjector injectors[];
  private TimeStampBuilder tsb;
  private AtomConfiguration conf;

  public AtomLogger(AtomConfiguration conf, AtomDataInjector... injectors) {
    super();
    this.conf = conf;
    this.injectors = injectors;
    init();
  }

  public void init() {
	  LOGGER.log(Level.INFO, "Initializing AtomLogger");
      tsb = new TimeStampBuilder(conf.getTsbDateBegin(), conf.getTsbOpenHour(), conf.getTsbCloseHour(), conf.getTsbNbTickMax(), conf.getNbAgents(), conf.getNbOrderBooks());
      tsb.init();
      for (int i = 0; i < injectors.length; i++) {
    	  injectors[i].createOutput();
      }
  }

  public void agentReferential(@NotNull List<AgentReferentialLine> referencial) throws
      IOException {
    assert !referencial.isEmpty();
    long ts = tsb.nextTimeStamp();
    for (int i = 0; i < injectors.length; i++) {
      injectors[i].sendAgentReferential(ts, referencial);
    }
  }

  @Override
  public void agent(Agent a, Order o, PriceRecord pr) {
    super.agent(a, o, pr);
    long ts = tsb.nextTimeStamp();
    for (int i = 0; i < injectors.length; i++) {
      injectors[i].sendAgent(ts, a, o, pr);
    }
  }


  @Override
  public void exec(Order o) {
    super.exec(o);
    long ts = tsb.nextTimeStamp();
    for (int i = 0; i < injectors.length; i++) {
      injectors[i].sendExec(ts, o);
    }
  }

  @Override
  public void order(Order o) {
    super.order(o);
    long ts = tsb.nextTimeStamp();
    for (int i = 0; i < injectors.length; i++) {
      injectors[i].sendOrder(ts, o);
    }
  }

  @Override
  public void price(PriceRecord pr, long bestAskPrice, long bestBidPrice) {
    super.price(pr, bestAskPrice, bestBidPrice);
    long ts = tsb.nextTimeStamp();
    for (int i = 0; i < injectors.length; i++) {
      injectors[i].sendPriceRecord(ts, pr, bestAskPrice, bestBidPrice);
    }
  }


  @Override
  public void day(int nbDays, java.util.Collection<OrderBook> orderbooks) {
    super.day(nbDays, orderbooks);
    
    tsb.setCurrentDay(nbDays);
    long ts = tsb.nextTimeStamp();
    for (int i = 0; i < injectors.length; i++) {
      injectors[i].sendDay(ts, nbDays, orderbooks);
    }
  }


  @Override
  public void tick(Day day, java.util.Collection<OrderBook> orderbooks) {
    super.tick(day, orderbooks);
    
	tsb.setCurrentTick(day.currentTick());
	tsb.setTimeStamp(tsb.baseTimeStampForCurrentTick());
    long ts = tsb.nextTimeStamp();
    for (int i = 0; i < injectors.length; i++) {
      injectors[i].sendTick(ts, day, orderbooks);
    }

  }

  public void close() throws Exception {
    for (int i = 0; i < injectors.length; i++) {
      injectors[i].closeOutput();
    }
  }

}