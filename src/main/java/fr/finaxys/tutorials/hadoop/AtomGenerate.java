package fr.finaxys.tutorials.hadoop;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import v13.Day;
import v13.MonothreadedSimulation;
import v13.Simulation;
import v13.agents.ZIT;
import fr.tutorials.utils.AtomConfiguration;
import fr.tutorials.utils.AtomDataInjector;
import fr.tutorials.utils.AtomLogger;
import fr.tutorials.utils.HadoopTutorialException;
import fr.tutorials.utils.LoggerStream;
import fr.tutorials.utils.avro.AvroInjector;
import fr.tutorials.utils.file.FileDataInjector;
import fr.tutorials.utils.hbase.SimpleHBaseInjector;

public class AtomGenerate {
	
  private static final Logger LOGGER = LogManager.getLogger(AtomGenerate.class.getName());
  
  // Static informations
  static private List<String> orderBooks;
  static private List<String> agents;
  
  private static v13.Logger logger = null;
  private static AtomConfiguration atomConf;

  // Main configuration for Atom
  public static void main(String args[]) throws IOException {
    // Loading properties
    try {
      getConfiguration();
    } catch (HadoopTutorialException e) {
      LOGGER.log(Level.ERROR, "Could not load properties", e);
      return;
    }
    // How long
    long startTime = System.currentTimeMillis();

    // Create simulator with custom logger
    
    try {
    	List<AtomDataInjector> injectors = new ArrayList<AtomDataInjector>();
	    if (atomConf.isOutHbase())
        {
	    	injectors.add(new SimpleHBaseInjector(atomConf));
        }
	    if (atomConf.isOutAvro()) {
        	injectors.add(new AvroInjector(atomConf));
	    } 
	    if (atomConf.isOutFile()) {
//	      PrintStream o = System.out;
//	      String outFile = System.getProperty("simul.output.file", "");
//	      if ("".equals(outFile)) {
//	      	o = new PrintStream(outFile);
//	      }
	    	PrintStream out = new PrintStream(new LoggerStream(LogManager.getLogger("atom"), Level.INFO));
	    	injectors.add(new FileDataInjector(out)); // new AtomLogger(atomConf);
	    }
	    logger = new AtomLogger(atomConf, injectors.toArray(new AtomDataInjector[injectors.size()]));
    } catch (Exception e) {
        LOGGER.log(Level.ERROR, "Could not instantiate logger", e);
        return;
      }
    Simulation sim = new MonothreadedSimulation();
    sim.setLogger(logger);
    
    //sim.setLogger(new FileLogger(System.getProperty("atom.output.file", "dump")));

    LOGGER.log(Level.INFO, "Setting up agents and orderbooks");

    // Create Agents and Order book to MarketMaker depending properties
    boolean marketmaker =  atomConf.isMarketMarker();
    int marketmakerQuantity = marketmaker ? atomConf.getMarketMakerQuantity() : 0;

    for (String agent : agents) {
      sim.addNewAgent(new ZIT(agent,
    		  atomConf.getAgentCash(),
    		  atomConf.getAgentMinPrice(),
    		  atomConf.getAgentMaxPrice(),
    		  atomConf.getAgentMinQuantity(),
    		  atomConf.getAgentMaxQuantity()));
    }
    for (int i = 0 ; i< orderBooks.size(); i++) {
      if (marketmaker) {
        sim.addNewMarketMaker(orderBooks.get(i) + "" + ((i % marketmakerQuantity) + 1));
      }
      sim.addNewOrderBook(orderBooks.get(i));
    }
    
    
    LOGGER.log(Level.INFO, "Launching simulation");

    sim.run(Day.createEuroNEXT(atomConf.getTickOpening(), atomConf.getTickContinuous(), 
    		atomConf.getTickClosing()), atomConf.getDays());

    LOGGER.log(Level.INFO, "Closing up");

    sim.market.close();
    
    if (logger instanceof AtomLogger) {
	    try {
	        ((AtomLogger)logger).close();
	      } catch (Exception e) {
	        LOGGER.log(Level.FATAL, "Could not close logger", e);
	        return;
	      }
    }

    long estimatedTime = System.currentTimeMillis() - startTime;
    LOGGER.info("Elapsed time: " + estimatedTime / 1000 + "s");
  }

  private static void getConfiguration() {

    atomConf = new AtomConfiguration();

    // Get agents & orderbooks
    agents = atomConf.getAgents();
    orderBooks = atomConf.getOrderBooks();

    if (agents.isEmpty() || orderBooks.isEmpty()) {
      LOGGER.log(Level.ERROR, "Agents/Orderbooks not set");
      throw new HadoopTutorialException("agents or orderbooks not set");
    }
  }
}