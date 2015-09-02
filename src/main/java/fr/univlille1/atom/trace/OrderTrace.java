package fr.univlille1.atom.trace;

public class OrderTrace implements Trace {

	private final TraceType type = TraceType.Order;
	private String orderBook;
	private String identifier;
	private OrderNature nature;
	private OrderDirection direction;
	private long price;
	private long quantity;
	private int validity;
	
	public OrderTrace() {}
	
	public OrderTrace(String orderBook, String identifier, OrderNature nature, OrderDirection direction,
			long price, long quantity, int validity) {
		super();
		this.orderBook = orderBook;
		this.identifier = identifier;
		this.nature = nature;
		this.direction = direction;
		this.price = price;
		this.quantity = quantity;
		this.validity = validity;
	}
	
	public TraceType getType() { return type; }

	public String getOrderBook() {
		return orderBook;
	}

	public void setOrderBook(String orderBook) {
		this.orderBook = orderBook;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public OrderNature getNature() {
		return nature;
	}

	public void setNature(OrderNature nature) {
		this.nature = nature;
	}

	public OrderDirection getDirection() {
		return direction;
	}

	public void setDirection(OrderDirection direction) {
		this.direction = direction;
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

	public int getValidity() {
		return validity;
	}

	public void setValidity(int validity) {
		this.validity = validity;
	}
}
