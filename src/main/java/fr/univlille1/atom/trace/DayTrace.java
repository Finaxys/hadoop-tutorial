package fr.univlille1.atom.trace;

public class DayTrace implements Trace {

	private final TraceType type = TraceType.Day;
	private long number;
	private String assetName;
	private long lastFixedPrice1;
	private long lastFixedPrice2;
	private long lastFixedPrice3;
	private long lastFixedPrice4;
	private long fixedPriceCount;

	public DayTrace() {
		super();
	}

	public DayTrace(long number, String assetName, long lastFixedPrice1,
			long lastFixedPrice2, long lastFixedPrice3, long lastFixedPrice4,
			long fixedPriceCount) {
		super();
		this.number = number;
		this.assetName = assetName;
		this.lastFixedPrice1 = lastFixedPrice1;
		this.lastFixedPrice2 = lastFixedPrice2;
		this.lastFixedPrice3 = lastFixedPrice3;
		this.lastFixedPrice4 = lastFixedPrice4;
		this.fixedPriceCount = fixedPriceCount;
	}


	public long getNumber() {
		return number;
	}


	public void setNumber(long number) {
		this.number = number;
	}


	public String getAssetName() {
		return assetName;
	}


	public void setAssetName(String assetName) {
		this.assetName = assetName;
	}


	public long getLastFixedPrice1() {
		return lastFixedPrice1;
	}


	public void setLastFixedPrice1(long lastFixedPrice1) {
		this.lastFixedPrice1 = lastFixedPrice1;
	}


	public long getLastFixedPrice2() {
		return lastFixedPrice2;
	}


	public void setLastFixedPrice2(long lastFixedPrice2) {
		this.lastFixedPrice2 = lastFixedPrice2;
	}


	public long getLastFixedPrice3() {
		return lastFixedPrice3;
	}


	public void setLastFixedPrice3(long lastFixedPrice3) {
		this.lastFixedPrice3 = lastFixedPrice3;
	}


	public long getLastFixedPrice4() {
		return lastFixedPrice4;
	}


	public void setLastFixedPrice4(long lastFixedPrice4) {
		this.lastFixedPrice4 = lastFixedPrice4;
	}


	public long getFixedPriceCount() {
		return fixedPriceCount;
	}


	public void setFixedPriceCount(long fixedPriceCount) {
		this.fixedPriceCount = fixedPriceCount;
	}


	public TraceType getType() { return type; }
	
}
