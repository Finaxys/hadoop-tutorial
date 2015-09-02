package fr.univlille1.atom.trace;

public class PriceTrace implements Trace {

	private final TraceType type = TraceType.Price;
	private String orderBook;
	private long price;
	private long quantity;
	private OrderDirection direction;
	private String initiatingOrderIdentifier;
	private String fullfillingOrderIdentifier;
	private long currentBestAsk;
	private long currentBestBid;
	
	public PriceTrace() {}
	
	public PriceTrace(String orderBook, long price, long quantity,
			OrderDirection direction, String initiatingOrderIdentifier,
			String fullfillingOrderIdentifier, long currentBestAsk,
			long currentBestBid) {
		super();
		this.orderBook = orderBook;
		this.price = price;
		this.quantity = quantity;
		this.direction = direction;
		this.initiatingOrderIdentifier = initiatingOrderIdentifier;
		this.fullfillingOrderIdentifier = fullfillingOrderIdentifier;
		this.currentBestAsk = currentBestAsk;
		this.currentBestBid = currentBestBid;
	}

	public TraceType getType() { return type; }

	public String getOrderBook() {
		return orderBook;
	}

	public void setOrderBook(String orderBook) {
		this.orderBook = orderBook;
	}

	public long getPrice() {
		return price;
	}

	public void setPrice(long price) {
		this.price = price;
	}

	public long getQuantity() {
		return quantity;
	}

	public void setQuantity(long quantity) {
		this.quantity = quantity;
	}

	public OrderDirection getDirection() {
		return direction;
	}

	public void setDirection(OrderDirection direction) {
		this.direction = direction;
	}

	public String getInitiatingOrderIdentifier() {
		return initiatingOrderIdentifier;
	}

	public void setInitiatingOrderIdentifier(String initiatingOrderIdentifier) {
		this.initiatingOrderIdentifier = initiatingOrderIdentifier;
	}

	public String getFullfillingOrderIdentifier() {
		return fullfillingOrderIdentifier;
	}

	public void setFullfillingOrderIdentifier(String fullfillingOrderIdentifier) {
		this.fullfillingOrderIdentifier = fullfillingOrderIdentifier;
	}

	public long getCurrentBestAsk() {
		return currentBestAsk;
	}

	public void setCurrentBestAsk(long currentBestAsk) {
		this.currentBestAsk = currentBestAsk;
	}

	public long getCurrentBestBid() {
		return currentBestBid;
	}

	public void setCurrentBestBid(long currentBestBid) {
		this.currentBestBid = currentBestBid;
	}
	
}
