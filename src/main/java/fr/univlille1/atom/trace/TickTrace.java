package fr.univlille1.atom.trace;

public class TickTrace implements Trace {
	
	private final TraceType type = TraceType.Tick;
	private long number;
	private String orderBook;
	private long bestAsk;
	private long bestBid;
	private long lastFixedPrice;
	
	public TickTrace() {}
	
	public TickTrace(long number, String orderBook, long bestAsk, long bestBid,
			long lastFixedPrice) {
		super();
		this.number = number;
		this.orderBook = orderBook;
		this.bestAsk = bestAsk;
		this.bestBid = bestBid;
		this.lastFixedPrice = lastFixedPrice;
	}

	public long getNumber() {
		return number;
	}

	public void setNumber(long number) {
		this.number = number;
	}

	public String getOrderBook() {
		return orderBook;
	}

	public void setOrderBook(String orderBook) {
		this.orderBook = orderBook;
	}

	public long getBestAsk() {
		return bestAsk;
	}

	public void setBestAsk(long bestAsk) {
		this.bestAsk = bestAsk;
	}

	public long getBestBid() {
		return bestBid;
	}

	public void setBestBid(long bestBid) {
		this.bestBid = bestBid;
	}

	public long getLastFixedPrice() {
		return lastFixedPrice;
	}

	public void setLastFixedPrice(long lastFixedPrice) {
		this.lastFixedPrice = lastFixedPrice;
	}

	public TraceType getType() { return type; }
	
}
