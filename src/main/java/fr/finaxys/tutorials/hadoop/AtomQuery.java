package fr.finaxys.tutorials.hadoop;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fr.finaxys.tutorials.utils.AtomAnalysis;
import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.HadoopTutorialException;
import fr.finaxys.tutorials.utils.hbase.HBaseAnalysis;

public class AtomQuery {
	
	private static final Logger LOGGER = LogManager
			.getLogger(AtomQuery.class.getName());
	
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
			
			AtomAnalysis analysis = new HBaseAnalysis();
			// @TODO a terminer
		}
		private static void getConfiguration() {

			atomConf = new AtomConfiguration();

		}
}
