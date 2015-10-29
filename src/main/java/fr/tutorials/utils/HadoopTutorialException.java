package fr.tutorials.utils;

public class HadoopTutorialException extends RuntimeException {
	
	private static final long serialVersionUID = 6811954454103190812L;

	public HadoopTutorialException() {
		super();
	}

	public HadoopTutorialException(String message, Throwable cause) {
		super(message, cause);
	}

	public HadoopTutorialException(String message) {
		super(message);
	}

	public HadoopTutorialException(Throwable cause) {
		super(cause);
	}
}
