package fr.tutorials.utils.hbase;


import com.sun.istack.NotNull;

import fr.tutorials.utils.AtomDataInjector;
import fr.tutorials.utils.AtomConfiguration;
import fr.tutorials.utils.HadoopTutorialException;
import v13.Day;
import v13.Logger;
import v13.Order;
import v13.OrderBook;
import v13.PriceRecord;
import v13.agents.Agent;

import java.io.PrintStream;
import java.util.List;
import java.util.logging.Level;

/**
 * @deprecated
 */
public class HBaseLogger extends Logger {
  private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(HBaseLogger.class.getName());

  AtomDataInjector injectors[];

  public HBaseLogger(AtomConfiguration conf, AtomDataInjector... injectors) throws Exception {
    super();
    if (conf.isOutFile()) {
      this.getClass().getField("pw").setAccessible(true);
      this.getClass().getField("pw").set(this, new PrintStream(conf.getOutFilePath()));
    }
    this.injectors = injectors;
    init();
  }

  public void init() {
    try {
      for (int i = 0; i < injectors.length; i++) {
        injectors[i].createOutput();
      }
    } catch (HadoopTutorialException e) {
      LOGGER.log(Level.SEVERE, "Could not create Connection", e);
      throw new HadoopTutorialException("Exception while initiating HBaseLogger", e.getCause());
    }
  }

  public void agentReferential(@NotNull List<AgentReferentialLine> referencial) {
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
    for (int i = 0; i < injectors.length; i++) {
      injectors[i].sendDay(nbDays, orderbooks);
    }
  }


  @Override
  public void tick(Day day, java.util.Collection<OrderBook> orderbooks) {
    super.tick(day, orderbooks);
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