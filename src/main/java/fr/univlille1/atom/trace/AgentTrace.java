package fr.univlille1.atom.trace;

public class AgentTrace implements Trace {

	private final TraceType type = TraceType.Agent;
	private String name;
	private long cash;
	private String assetName;
	private long assetQuantity;
	private long currentPrice;
	
	public TraceType getType() { return type; }
	
	public AgentTrace() {}

	public AgentTrace(String name, long cash, String assetName,
			long assetQuantity, long currentPrice) {
		super();
		this.name = name;
		this.cash = cash;
		this.assetName = assetName;
		this.assetQuantity = assetQuantity;
		this.currentPrice = currentPrice;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getCash() {
		return cash;
	}

	public void setCash(long cash) {
		this.cash = cash;
	}

	public String getAssetName() {
		return assetName;
	}

	public void setAssetName(String assetName) {
		this.assetName = assetName;
	}

	public long getAssetQuantity() {
		return assetQuantity;
	}

	public void setAssetQuantity(long assetQuantity) {
		this.assetQuantity = assetQuantity;
	}

	public long getCurrentPrice() {
		return currentPrice;
	}

	public void setCurrentPrice(long currentPrice) {
		this.currentPrice = currentPrice;
	}

	
	
}
