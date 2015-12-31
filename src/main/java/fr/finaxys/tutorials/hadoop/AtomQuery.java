package fr.finaxys.tutorials.hadoop;

import java.io.IOException;
import java.util.Date;

import fr.finaxys.tutorials.utils.AtomAnalysis;
import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.hbase.HBaseAnalysis;

public class AtomQuery {
	
	// Main configuration for Atom
	public static void main(String args[]) throws IOException {
		// Loading properties
		
		AtomConfiguration atomConf = AtomConfiguration.getInstance();
		AtomAnalysis analysis = new HBaseAnalysis();
		analysis.agentPosition(new Date());
		// @TODO a terminer
	}

}
