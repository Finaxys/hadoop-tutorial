package fr.finaxys.tutorials.utils.hbase;

import com.sun.istack.NotNull;
import fr.finaxys.tutorials.utils.AgentReferentialLine;
import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.AtomDataInjector;
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

	private int dayGap;

	private final AtomicLong globalCount = new AtomicLong(0L);
	
	public SimpleHBaseInjector(@NotNull AtomConfiguration conf) {
		super(conf.getColumnFamily(), TableName.valueOf(conf.getTableName()),conf);
		this.dayGap = conf.getDayGap();
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
	public void sendAgent(long ts, Agent a, Order o, PriceRecord pr) {
		Put p = mkPutAgent(createRequired(AGENT), ts, a, o, pr);
		putTable(p);
	}
	
	@Override
	public void sendAgentReferential(long ts, List<AgentReferentialLine> referencial) {
		for (AgentReferentialLine agent : referencial) {
			Put p = mkPutAgentReferential(agent, hbEncoder, columnFamily,
					System.currentTimeMillis());
			putTable(p);
		}
	}
	
	@Override
	public void sendDay(long ts, int nbDays, Collection<OrderBook> orderbooks) {
		for (OrderBook ob : orderbooks) {
			Put p = mkPutDay(createRequired(DAY), ts, dayGap, nbDays, ob);
			putTable(p);
		}
	}
	
	@Override
	public void sendExec(long ts, Order o) {
		Put p = mkPutExec(createRequired(EXEC), ts, o);
		putTable(p);
	}
	
	@Override
	public void sendOrder(long ts, Order o) {
		
		// hack for update on scaledrisk
		// (does not manage put then
		// update with same ts)
		//long ts = System.currentTimeMillis(); 
		o.timestamp = ts;
		Put p = mkPutOrder(createRequired(ORDER), o.timestamp, o);
		putTable(p);
	}
	
	@Override
	public void sendPriceRecord(long ts, PriceRecord pr, long bestAskPrice,
			long bestBidPrice) {
		// hack for update on
		// scaledrisk (does not
		// manage put then update
		// with same ts)
		//long ts = System.currentTimeMillis() + 2L; 
		pr.timestamp = ts;

		Put p = mkPutPriceRecord(createRequired(PRICE), pr.timestamp, pr, bestAskPrice, bestBidPrice);
		putTable(p);
	}
	
	@Override
	public void sendTick(long ts, Day day, Collection<OrderBook> orderbooks) {
		for (OrderBook ob : orderbooks) {
			Put p = mkPutTick(createRequired(TICK), ts, dayGap, day, ob);
			putTable(p);
		}
	}

	public int getDayGap() {
		return dayGap;
	}

	public void setDayGap(int dayGap) {
		this.dayGap = dayGap;
	}

}
