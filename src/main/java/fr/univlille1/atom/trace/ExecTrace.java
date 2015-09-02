package fr.univlille1.atom.trace;

public class ExecTrace implements Trace {

	private final TraceType type = TraceType.Exec;
	private String orderIdentifier;
	
	public TraceType getType() { return type ; }
	
	public ExecTrace() {}

	public ExecTrace(String orderIdentifier) {
		super();
		this.orderIdentifier = orderIdentifier;
	}

	public String getOrderIdentifier() {
		return orderIdentifier;
	}

	public void setOrderIdentifier(String orderIdentifier) {
		this.orderIdentifier = orderIdentifier;
	}

}
