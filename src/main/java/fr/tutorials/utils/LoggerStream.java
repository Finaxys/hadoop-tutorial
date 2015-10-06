package fr.tutorials.utils;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

public class LoggerStream extends OutputStream {
	
	private final Logger logger;
	private final Level logLevel;

	public LoggerStream(Logger logger, Level logLevel) {
		super();

		this.logger = logger;
		this.logLevel = logLevel;
	}

	@Override
	public void write(byte[] b) throws IOException {
		String string = new String(b);
		if (!string.trim().isEmpty() && string.charAt(0) != '!')
			logger.log(logLevel, string);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		String string = new String(b, off, len);
		if (!string.trim().isEmpty() && string.charAt(0) != '!')
			logger.log(logLevel, string);
	}

	@Override
	public void write(int b) throws IOException {
		String string = String.valueOf((char) b);
		if (!string.trim().isEmpty() && string.charAt(0) != '!')
			logger.log(logLevel, string);
	}
}