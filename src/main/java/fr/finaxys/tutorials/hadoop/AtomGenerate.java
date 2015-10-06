package fr.finaxys.tutorials.hadoop;

import fr.tutorials.utils.FileLogger;
import v13.Day;
import v13.MonothreadedSimulation;
import v13.Simulation;
import v13.agents.ZIT;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fr.tutorials.utils.LoggerStream;

public class AtomGenerate {
	
  private static final Logger LOGGER = LogManager.getLogger(AtomGenerate.class.getName());
  
  // Static informations
 // static public final String[] DOW2 = {"MMM", "AXP"};
 // static public final String[] DOW30 = {"MMM", "AXP", "AAPL", "BA", "CAT", "CVX", "CSCO", "KO", "DIS", "DD", "XOM", "GE", "GS", "HD", "IBM", "INTC", "JNJ", "JPM", "MCD", "MRK", "MSFT", "NKE", "PFE", "PG", "TRV", "UTX", "UNH", "VZ", "V", "WMT"};
  static private List<String> orderBooks;
  static private List<String> agents;

  // Main configuration for Atom
  public static void main(String args[]) throws IOException {
    // Loading properties
    try {
      getConfiguration();
    } catch (Exception e) {
      LOGGER.log(Level.ERROR, "Could not load properties", e);
      return;
    }
//    PrintStream o = System.out;
//    String outFile = System.getProperty("simul.output.file", "");
//    if ("".equals(outFile)) {
//    	o = new PrintStream(outFile);
//    }
    PrintStream out = new PrintStream(new LoggerStream(LogManager.getLogger("atom"), Level.INFO));
    //PrintStream out = System.out;

    // How long
    long startTime = System.currentTimeMillis();

    // Create simulator with custom logger
    Simulation sim = new MonothreadedSimulation();
    sim.setLogger(new FileLogger(out));
    //sim.setLogger(new FileLogger(System.getProperty("atom.output.file", "dump")));

    LOGGER.log(Level.INFO, "Setting up agents and orderbooks");

    // Create Agents and Order book to MarketMaker depending properties
    boolean marketmaker = System.getProperty("atom.marketmaker", "true").equals("true");
    int marketmakerQuantity = marketmaker ? Integer.parseInt(System.getProperty("atom.marketmaker.quantity", "1")) : 0;

    for (String agent : agents) {
      sim.addNewAgent(new ZIT(agent, Integer.parseInt(System.getProperty("simul. .cash", "0")),
          Integer.parseInt(System.getProperty("simul.agent.minprice", "10000")),
          Integer.parseInt(System.getProperty("simul.agent.maxprice", "20000")),
          Integer.parseInt(System.getProperty("simul.agent.minquantity", "10")),
          Integer.parseInt(System.getProperty("simul.agent.maxquantity", "50"))));
    }
    for (int i = 0 ; i< orderBooks.size(); i++) {
      if (marketmaker) {
        sim.addNewMarketMaker(orderBooks.get(i) + "" + ((i % marketmakerQuantity) + 1));
      }
      sim.addNewOrderBook(orderBooks.get(i));
    }
    LOGGER.log(Level.INFO, "Launching simulation");

    sim.run(Day.createEuroNEXT(Integer.parseInt(System.getProperty("simul.tick.opening", "0")),
            Integer.parseInt(System.getProperty("simul.tick.continuous", "10")),
            Integer.parseInt(System.getProperty("simul.tick.closing", "0"))),
        Integer.parseInt(System.getProperty("simul.days", "1")));

    LOGGER.log(Level.INFO, "Closing up");

    sim.market.close();

    long estimatedTime = System.currentTimeMillis() - startTime;
    LOGGER.info("Elapsed time: " + estimatedTime / 1000 + "s");
  }

  private static void getConfiguration() throws Exception {
    FileInputStream propFile = new FileInputStream("properties.txt");
    Properties p = new Properties(System.getProperties());
    p.load(propFile);
    System.setProperties(p);

    // Get agents & orderbooks
    String obsym = System.getProperty("atom.orderbooks", "");
    String agsym = System.getProperty("atom.agents", "");

    if ("random".equals(agsym)) {
    	int agsize = Integer.parseInt(System.getProperty("atom.agents.random", "1000"));
    	agents = new ArrayList<String>(agsize);
    	for (int i = 0; i < agsize; i++) {
    		agents.add("Agent"+i);
    	}
    } else {
    	agents = Arrays.asList(System.getProperty("symbols.agents." + agsym, "").split("\\s*,\\s*"));
    }
    if ("random".equals(obsym)) {
    	int obsize = Integer.parseInt(System.getProperty("atom.orderbooks.random", "100"));
    	orderBooks = new ArrayList<String>(obsize);
    	for (int i = 0; i < obsize; i++) {
    		orderBooks.add("Orderbook"+i);
    	}
    } else {
    	orderBooks = Arrays.asList(System.getProperty("symbols.orderbooks." + obsym, "").split("\\s*,\\s*"));
    }

    if (agents.isEmpty() || orderBooks.isEmpty()) {
      LOGGER.log(Level.ERROR, "Agents/Orderbooks not set");
      throw new Exception("agents or orderbooks not set");
    }
  }
}