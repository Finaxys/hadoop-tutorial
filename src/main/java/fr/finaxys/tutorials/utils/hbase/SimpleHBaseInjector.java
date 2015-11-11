package fr.finaxys.tutorials.utils.hbase;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import v13.Day;
import v13.Order;
import v13.OrderBook;
import v13.PriceRecord;
import v13.agents.Agent;

import com.sun.istack.NotNull;

import fr.finaxys.tutorials.utils.AgentReferentialLine;
import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.AtomDataInjector;
import fr.finaxys.tutorials.utils.TimeStampBuilder;

/**
 *
 */
public class SimpleHBaseInjector extends AtomHBaseHelper implements AtomDataInjector {

	private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
			.getLogger(SimpleHBaseInjector.class.getName());
	
	private TimeStampBuilder timeStampBuilder;

	int dayGap;
	private AtomConfiguration atomConfiguration;

	private final AtomicLong globalCount = new AtomicLong(0L);
	private AtomicLong idGen = new AtomicLong(1_000_000);

	public SimpleHBaseInjector(@NotNull AtomConfiguration conf) {
		super(conf.getColumnFamily(), TableName.valueOf(conf.getTableName()));
		
		this.atomConfiguration = conf;
		this.dayGap = atomConfiguration.getDayGap();
		/* this.table = createHTableConnexion(tableName
				, this.hbaseConfiguration); */
	}
	
	public SimpleHBaseInjector() {
		
	}

	@Override
	public void closeOutput() {
		try {
			closeTable();
		} finally {
			LOGGER.info("Total put sent : " + globalCount.get());
		}
	}
	
	@Override
	protected void putTable(@NotNull Put p) {
		super.putTable(p);
		globalCount.addAndGet(1);
	}


	@Override
	public void createOutput() {
		openTable();
	}

	@NotNull byte[] createRequired(@NotNull char name) {
		long rowKey = Long.reverseBytes(idGen.incrementAndGet());
		return Bytes.toBytes(String.valueOf(rowKey) + name);
	}
	
	/**
	 * Absolutely not thread safe. Only used for Unit test
	 * @param name
	 * @return
	 */
	protected byte[] getLastRequired(@NotNull char name) {
		long rowKey = Long.reverseBytes(idGen.get());
		return Bytes.toBytes(String.valueOf(rowKey) + name);
	}

	@Override
	public void sendAgent(Agent a, Order o, PriceRecord pr) {
		Put p = mkPutAgent(createRequired(AGENT), a, o, pr);
		putTable(p);
	}
	
	@Override
	public void sendAgentReferential(List<AgentReferentialLine> referencial) {
		for (AgentReferentialLine agent : referencial) {
			Put p = mkPutAgentReferential(agent, hbEncoder, columnFamily,
					System.currentTimeMillis());
			putTable(p);
		}
	}
	
	@Override
	public void sendDay(int nbDays, Collection<OrderBook> orderbooks) {
		for (OrderBook ob : orderbooks) {
			Put p = mkPutOrderBook(createRequired(DAY), dayGap, nbDays, ob);
			putTable(p);
		}
	}
	
	@Override
	public void sendExec(Order o) {
		Put p = mkPutExec(createRequired(EXEC), o);
		putTable(p);
	}
	
	@Override
	public void sendOrder(Order o) {
		o.timestamp = timeStampBuilder.nextTimeStamp();
		long ts = System.currentTimeMillis(); // hack for update on scaledrisk
												// (does not manage put then
												// update with same ts)
		Put p = mkPutOrder(createRequired(ORDER), ts, o);
		putTable(p);
	}
	
	@Override
	public void sendPriceRecord(PriceRecord pr, long bestAskPrice,
			long bestBidPrice) {
		long ts = System.currentTimeMillis() + 2L; // hack for update on
													// scaledrisk (does not
													// manage put then update
													// with same ts)
		pr.timestamp = timeStampBuilder.nextTimeStamp();

		Put p = mkPutPriceRecord(createRequired(PRICE), ts, pr, bestAskPrice, bestBidPrice);
		putTable(p);
	}
	
	@Override
	public void sendTick(Day day, Collection<OrderBook> orderbooks) {
		for (OrderBook ob : orderbooks) {
			Put p = mkPutTick(createRequired(TICK), dayGap, day, ob);
			putTable(p);
		}
	}
	
	public void setAtomConfiguration(AtomConfiguration atomConfiguration) {
		this.atomConfiguration = atomConfiguration;
	}

	public int getDayGap() {
		return dayGap;
	}

	public void setDayGap(int dayGap) {
		this.dayGap = dayGap;
	}
	
	public AtomConfiguration getAtomConfiguration() {
		return atomConfiguration;
	}

	public TimeStampBuilder getTimeStampBuilder() {
		return timeStampBuilder;
	}
	
	@Override
	public void setTimeStampBuilder(TimeStampBuilder tsb) {
		this.timeStampBuilder = tsb;
	}
}
