package fr.finaxys.tutorials.hadoop;

import fr.finaxys.tutorials.utils.*;
import fr.finaxys.tutorials.utils.avro.AvroInjector;
import fr.finaxys.tutorials.utils.file.FileDataInjector;
import fr.finaxys.tutorials.utils.hbase.SimpleHBaseInjector;
import fr.finaxys.tutorials.utils.kafka.KafkaInjector;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import v13.Day;
import v13.MonothreadedSimulation;
import v13.Simulation;
import v13.agents.ZIT;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AtomGenerate {

	private static final Logger LOGGER = LogManager
			.getLogger(AtomGenerate.class.getName());

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

		List<String> parseArgs = Arrays.asList(args);

		// How long
		long startTime = System.currentTimeMillis();

		// Create simulator with custom logger

		List<AtomDataInjector> injectors = new ArrayList<AtomDataInjector>();
		try {
			if (parseArgs.contains("-hbase") || atomConf.isOutHbase()) {
				injectors.add(new SimpleHBaseInjector(atomConf));
			}
			if (parseArgs.contains("-avro") || atomConf.isOutAvro()) {
				injectors.add(new AvroInjector(atomConf));
			}
			if (parseArgs.contains("-file") || atomConf.isOutFile()) {
				PrintStream out = new PrintStream(new LoggerStream(
						LogManager.getLogger("atom"), Level.INFO));
				injectors.add(new FileDataInjector(out)); // new
															// AtomLogger(atomConf);
			}
            if (parseArgs.contains("-kafka") || atomConf.isOutKafka()) {
                injectors.add(new KafkaInjector(atomConf));
            }

		} catch (Exception e) {
			LOGGER.log(Level.ERROR, "Could not instantiate logger", e);
			return;
		}
		if (injectors.isEmpty()) {
			throw new HadoopTutorialException("No output define");
		}

		logger = new AtomLogger(atomConf,
				injectors.toArray(new AtomDataInjector[injectors.size()]));
		Simulation sim = new MonothreadedSimulation();
		sim.setLogger(logger);

		// sim.setLogger(new FileLogger(System.getProperty("atom.output.file",
		// "dump")));

		LOGGER.log(Level.INFO, "Setting up agents and orderbooks");

		// Create Agents and Order book to MarketMaker depending properties
		boolean marketmaker = atomConf.isMarketMarker();
		int marketmakerQuantity = marketmaker ? atomConf
				.getMarketMakerQuantity() : 0;

		for (String agent : agents) {
			sim.addNewAgent(new ZIT(agent, atomConf.getAgentCash(), atomConf
					.getAgentMinPrice(), atomConf.getAgentMaxPrice(), atomConf
					.getAgentMinQuantity(), atomConf.getAgentMaxQuantity()));
		}
		for (int i = 0; i < orderBooks.size(); i++) {
			if (marketmaker) {
				sim.addNewMarketMaker(orderBooks.get(i) + ""
						+ ((i % marketmakerQuantity) + 1));
			}
			sim.addNewOrderBook(orderBooks.get(i));
		}

		LOGGER.log(Level.INFO, "Launching simulation");

		sim.run(Day.createEuroNEXT(atomConf.getTickOpening(),
				atomConf.getTickContinuous(), atomConf.getTickClosing()),
				atomConf.getDays());

		LOGGER.log(Level.INFO, "Closing up");

		sim.market.close();

		if (logger instanceof AtomLogger) {
			try {
				((AtomLogger) logger).close();
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