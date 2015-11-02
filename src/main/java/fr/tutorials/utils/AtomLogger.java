package fr.tutorials.utils;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import v13.Day;
import v13.Logger;
import v13.Order;
import v13.OrderBook;
import v13.PriceRecord;
import v13.agents.Agent;

import com.sun.istack.NotNull;

import fr.tutorials.utils.hbase.AgentReferentialLine;

public class AtomLogger extends Logger {
  private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(AtomLogger.class.getName());

  private AtomDataInjector injectors[];
  private TimeStampBuilder tsb;

  public AtomLogger(AtomConfiguration conf, AtomDataInjector... injectors) throws Exception {
    super();
//    if (conf.isOutFile()) {
//      this.getClass().getField("pw").setAccessible(true);
//      this.getClass().getField("pw").set(this, new PrintStream(conf.getOutFilePath()));
//    }
    this.injectors = injectors;
    init();
  }

  public void init() {
	  try {
          tsb = new TimeStampBuilder();
          tsb.loadConfig();
          tsb.init();
      } catch (ParseException e) {
      	 throw new HadoopTutorialException("Cannot init FileDataInjector", e);
		}
	  
      for (int i = 0; i < injectors.length; i++) {
    	  injectors[i].setTimeStampBuilder(tsb);
    	  injectors[i].createOutput();
      }
  }

  public void agentReferential(@NotNull List<AgentReferentialLine> referencial) throws
      IOException {
    assert !referencial.isEmpty();
    for (int i = 0; i < injectors.length; i++) {
      injectors[i].sendAgentReferential(referencial);
    }
  }

  @Override
  public void agent(Agent a, Order o, PriceRecord pr) {
    super.agent(a, o, pr);
    for (int i = 0; i < injectors.length; i++) {
      injectors[i].sendAgent(a, o, pr);
    }
  }


  @Override
  public void exec(Order o) {
    super.exec(o);
    for (int i = 0; i < injectors.length; i++) {
      injectors[i].sendExec(o);
    }
  }

  @Override
  public void order(Order o) {
    super.order(o);
    for (int i = 0; i < injectors.length; i++) {
      injectors[i].sendOrder(o);
    }
  }

  @Override
  public void price(PriceRecord pr, long bestAskPrice, long bestBidPrice) {
    super.price(pr, bestAskPrice, bestBidPrice);
    for (int i = 0; i < injectors.length; i++) {
      injectors[i].sendPriceRecord(pr, bestAskPrice, bestBidPrice);
    }
  }


  @Override
  public void day(int nbDays, java.util.Collection<OrderBook> orderbooks) {
    super.day(nbDays, orderbooks);
    
    tsb.setCurrentDay(nbDays);
    
    for (int i = 0; i < injectors.length; i++) {
      injectors[i].sendDay(nbDays, orderbooks);
    }
  }


  @Override
  public void tick(Day day, java.util.Collection<OrderBook> orderbooks) {
    super.tick(day, orderbooks);
    
	tsb.setCurrentTick(day.currentTick());
	tsb.setTimeStamp(tsb.baseTimeStampForCurrentTick());
	
    for (int i = 0; i < injectors.length; i++) {
      injectors[i].sendTick(day, orderbooks);
    }

  }

  public void close() throws Exception {
    for (int i = 0; i < injectors.length; i++) {
      injectors[i].close();
    }
  }

}