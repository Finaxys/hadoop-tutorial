package fr.finaxys.tutorials.utils.hbase;

import com.sun.istack.NotNull;
import fr.finaxys.tutorials.utils.AgentReferentialLine;
import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.AtomDataInjector;
import fr.finaxys.tutorials.utils.TimeStampBuilder;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import v13.Day;
import v13.Order;
import v13.OrderBook;
import v13.PriceRecord;
import v13.agents.Agent;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class SimpleHBaseInjector extends AtomHBaseHelper implements AtomDataInjector {

	private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
			.getLogger(SimpleHBaseInjector.class.getName());
	
	private TimeStampBuilder timeStampBuilder;

	private int dayGap;
	private AtomConfiguration atomConfiguration;

	private final AtomicLong globalCount = new AtomicLong(0L);
	
	public SimpleHBaseInjector(@NotNull AtomConfiguration conf) {
		super(conf.getColumnFamily(), TableName.valueOf(conf.getTableName()),conf);
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

	@Override
	public void sendAgent(Agent a, Order o, PriceRecord pr) {
		long ts = timeStampBuilder.nextTimeStamp();
		Put p = mkPutAgent(createRequired(AGENT), ts, a, o, pr);
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
		long ts = timeStampBuilder.nextTimeStamp();
		for (OrderBook ob : orderbooks) {
			Put p = mkPutDay(createRequired(DAY), ts, dayGap, nbDays, ob);
			putTable(p);
		}
	}
	
	@Override
	public void sendExec(Order o) {
		long ts = timeStampBuilder.nextTimeStamp();
		Put p = mkPutExec(createRequired(EXEC), ts, o);
		putTable(p);
	}
	
	@Override
	public void sendOrder(Order o) {
		
		// hack for update on scaledrisk
		// (does not manage put then
		// update with same ts)
		//long ts = System.currentTimeMillis(); 
		o.timestamp = timeStampBuilder.nextTimeStamp();
		Put p = mkPutOrder(createRequired(ORDER), o.timestamp, o);
		putTable(p);
	}
	
	@Override
	public void sendPriceRecord(PriceRecord pr, long bestAskPrice,
			long bestBidPrice) {
		// hack for update on
		// scaledrisk (does not
		// manage put then update
		// with same ts)
		//long ts = System.currentTimeMillis() + 2L; 
		pr.timestamp = timeStampBuilder.nextTimeStamp();

		Put p = mkPutPriceRecord(createRequired(PRICE), pr.timestamp, pr, bestAskPrice, bestBidPrice);
		putTable(p);
	}
	
	@Override
	public void sendTick(Day day, Collection<OrderBook> orderbooks) {
		long ts = timeStampBuilder.nextTimeStamp();
		for (OrderBook ob : orderbooks) {
			Put p = mkPutTick(createRequired(TICK), ts, dayGap, day, ob);
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
