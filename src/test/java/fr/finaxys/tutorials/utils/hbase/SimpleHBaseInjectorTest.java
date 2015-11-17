package fr.finaxys.tutorials.utils.hbase;

import fr.finaxys.tutorials.utils.InjectorTests;
import fr.finaxys.tutorials.utils.TimeStampBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import v13.*;
import v13.agents.Agent;
import v13.agents.DumbAgent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import static fr.finaxys.tutorials.utils.hbase.SimpleHBaseInjector.*;

@Category(InjectorTests.class)
public class SimpleHBaseInjectorTest {
	
	private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
			.getLogger(SimpleHBaseInjectorTest.class.getName());
	
	private static final TableName TEST_TABLE = TableName
			.valueOf("testhbaseinjector");
	private static final byte[] TEST_FAMILY = Bytes.toBytes("f");
	//private static final byte[] TEST_COLUMN = Bytes.toBytes("col");
	private static final int DAY_GAP = 2;

	private static HBaseTestingUtility TEST_UTIL = null;
	private static Configuration CONF = null;
	private static Table table = null;
	
	final HBaseDataTypeEncoder hbEncoder = new HBaseDataTypeEncoder();

	private SimpleHBaseInjector injector = new SimpleHBaseInjector();
	private TimeStampBuilder tsb = null;

	@BeforeClass
	public static void setupBeforeClass() throws Exception {
		TEST_UTIL = new HBaseTestingUtility();
		CONF = TEST_UTIL.getConfiguration();

		TEST_UTIL.startMiniCluster();
		table = TEST_UTIL.createTable(TEST_TABLE, new byte[][] { TEST_FAMILY });
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		TEST_UTIL.shutdownMiniCluster();
	}

	@Before
	public void setUp() {
		tsb = new TimeStampBuilder("09/13/1986", "9:00", "17:30", 3000, 2, 2);
		tsb.init();
		injector.setTimeStampBuilder(tsb);
		injector.setHbaseConfiguration(CONF);
		injector.setTableName(TEST_TABLE);
		injector.setColumnFamily(TEST_FAMILY);
		injector.setDayGap(DAY_GAP);
		injector.createOutput();
	}

	@After
	public void tearDown() {
		injector.closeOutput();
	}

	@Test
	public void testSendAgent() {
			// Agent a, Order o, PriceRecord pr
		try {
			Agent a = new DumbAgent("a");
			Order o = new LimitOrder("o", "1", LimitOrder.ASK, 1, 10);
			o.sender = a;
			PriceRecord pr = new PriceRecord("o", 10, 1, LimitOrder.ASK, "o-1", "o-2");
			injector.sendAgent(a, o, pr);
			String reference = Bytes.toString(injector.getLastRequired(AtomHBaseHelper.AGENT));
			LOGGER.log(Level.INFO, "Agent reference :"+reference);
			Get g = new Get(Bytes.toBytes(reference));
			g.addFamily(TEST_FAMILY);
			Result result = table.get(g);
			Assert.assertTrue("Can retrieve agent by his reference", Bytes.equals(result.getRow(), Bytes.toBytes(reference)));
			String agentName = hbEncoder.decodeString(result.getValue(TEST_FAMILY, Q_AGENT_NAME));
			Assert.assertTrue("AgentName is same", agentName.equals(a.name));
            String orderBookName = hbEncoder.decodeString(result.getValue(TEST_FAMILY, Q_OB_NAME));
			Assert.assertTrue("orderBookName is same", orderBookName.equals(o.obName));
			int executedQuantity = hbEncoder.decodeInt(result.getValue(TEST_FAMILY, Q_EXECUTED_QUANTITY));
			Assert.assertTrue("executedQuantity is same", executedQuantity == pr.quantity);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "IO Exception on testSendAgent", e);
		}
	}

	@Test
	public void testSendPriceRecord() {
		// PriceRecord pr, long bestAskPrice, long bestBidPrice
		try {
			PriceRecord pr = new PriceRecord("pr", 10, 1, LimitOrder.ASK, "o-1", "o-2");
			long bestAskPrice = 1;
			long bestBidPrice = 2;
			injector.sendPriceRecord(pr, bestAskPrice, bestBidPrice);
			String reference = Bytes.toString(injector.getLastRequired(AtomHBaseHelper.PRICE));
			LOGGER.log(Level.INFO, "Price Record reference :"+reference);
			Get g = new Get(Bytes.toBytes(reference));
			g.addFamily(TEST_FAMILY);
			Result result = table.get(g);
			Assert.assertTrue("Can retrieve pricerecord by his reference", Bytes.equals(result.getRow(), Bytes.toBytes(reference)));
			String orderBookName = hbEncoder.decodeString(result.getValue(TEST_FAMILY, Q_OB_NAME));
			Assert.assertTrue("orderBookName is same", orderBookName.equals( pr.obName));
			long bestAsk = hbEncoder.decodeLong(result.getValue(TEST_FAMILY, Q_BEST_ASK));
			Assert.assertTrue("executedQuantity is same", bestAsk==bestAskPrice);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "IO Exception on testSendAgent", e);
		}
	}

	@Test
	public void testSendAgentReferential() {
		// List<AgentReferentialLine> referencial
		System.out.println("HERE!!!!");
	}

	@Test
	public void testSendOrder() {
		// Order o
		try {
			LimitOrder o = new LimitOrder("o", "1", LimitOrder.ASK, 1, 10);
			o.sender = new DumbAgent("a");
			injector.sendOrder(o);
			String reference = Bytes.toString(injector.getLastRequired(AtomHBaseHelper.ORDER));
			LOGGER.log(Level.INFO, "Order reference :"+reference);
			Get g = new Get(Bytes.toBytes(reference));
			g.addFamily(TEST_FAMILY);
			Result result = table.get(g);
			Assert.assertTrue("Can retrieve pricerecord by his reference", Bytes.equals(result.getRow(), Bytes.toBytes(reference)));
			byte[] orderBookName = result.getValue(TEST_FAMILY, Q_OB_NAME);
			byte[] orderBookName2 = hbEncoder.encodeString(o.obName);
			Assert.assertTrue("orderBookName is same", Bytes.equals(orderBookName, orderBookName2));
			char dir = hbEncoder.decodeChar(result.getValue(TEST_FAMILY, Q_DIRECTION));
			Assert.assertTrue("orderBookName is same", dir == o.direction);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "IO Exception on testSendAgent", e);
		}
	}

	@Test
	public void testSendTick() {
		// Day day, Collection<OrderBook> orderbooks
		try {
/*			List<Period> periods = new ArrayList<Period>();
			periods.add(new Period());*/
			Day day = Day.createSinglePeriod(1, 100);
			day.nextPeriod();
			OrderBook ob = new OrderBook("ob1");
			OrderBook ob2 = new OrderBook("ob2");
			List<OrderBook> obs = new ArrayList<OrderBook>();
			obs.add(ob);
			obs.add(ob2);
			injector.sendTick(day, obs);
			String reference = Bytes.toString(injector.getLastRequired(AtomHBaseHelper.TICK));
			LOGGER.log(Level.INFO, "Tick reference :"+reference);
			// WILL RETRIEVE LAST TICK ONLY
			Get g = new Get(Bytes.toBytes(reference));
			g.addFamily(TEST_FAMILY);
			Result result = table.get(g);
			Assert.assertTrue("Can retrieve pricerecord by his reference", Bytes.equals(result.getRow(), Bytes.toBytes(reference)));
			String orderBookName = hbEncoder.decodeString(result.getValue(TEST_FAMILY, Q_OB_NAME));
			Assert.assertTrue("orderBookName is same", orderBookName.equals(ob2.obName));
			int dayGap = hbEncoder.decodeInt(result.getValue(TEST_FAMILY, EXT_NUM_DAY));
			Assert.assertTrue("Day Gap is same", dayGap ==  day.number + DAY_GAP);
			int tick = hbEncoder.decodeInt(result.getValue(TEST_FAMILY, Q_NUM_TICK));
			Assert.assertTrue("Tick is same", tick == day.currentTick());
			
			
			List<Pair<byte[], byte[]>> keys = new ArrayList<Pair<byte[], byte[]>>();
		    keys.add(new Pair<byte[], byte[]>(
		    		Bytes.toBytes("???????????????????"+AtomHBaseHelper.TICK),
		    		new byte[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0 }));
		    Filter filter = new FuzzyRowFilter(keys);
		    
			Scan s = new Scan();
			s.addFamily(TEST_FAMILY);
			s.setFilter(filter);
			ResultScanner results = table.getScanner(s);
			int i = 0;
			for (Result r : results) i++;
			Assert.assertEquals("2 order books expected", i, obs.size());
			
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "IO Exception on testSendAgent", e);
		}
	}

	@Test
	public void testSendDay() {
		// int nbDays, Collection<OrderBook> orderbooks
		try {
			int day = 1;
			OrderBook ob = new OrderBook("ob1");
			OrderBook ob2 = new OrderBook("ob2");
			List<OrderBook> obs = new ArrayList<OrderBook>();
			obs.add(ob);
			obs.add(ob2);
			injector.sendDay(day, obs);
			String reference = Bytes.toString(injector.getLastRequired(AtomHBaseHelper.DAY));
			// WILL RETRIEVE LAST DAY ONLY
			Get g = new Get(Bytes.toBytes(reference));
			g.addFamily(TEST_FAMILY);
			Result result = table.get(g);
			Assert.assertNotNull("Can retrieve pricerecord by his reference", Bytes.equals(result.getRow(), Bytes.toBytes(reference)));
			String orderBookName = hbEncoder.decodeString(result.getValue(TEST_FAMILY, Q_OB_NAME));
			Assert.assertTrue("orderBookName is same", orderBookName.equals(ob2.obName));
			int dayGap = hbEncoder.decodeInt(result.getValue(TEST_FAMILY, EXT_NUM_DAY));
			Assert.assertTrue("Day Gap is same", dayGap == day + DAY_GAP);
			
			List<Pair<byte[], byte[]>> keys = new ArrayList<Pair<byte[], byte[]>>();
			
		    keys.add(new Pair<byte[], byte[]>(
		    		Bytes.toBytes("???????????????????"+AtomHBaseHelper.DAY),
		    		new byte[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0 }));
		    Filter filter = new FuzzyRowFilter(keys);
			
			Scan s = new Scan();
			s.addFamily(TEST_FAMILY);
			s.setFilter(filter);
			ResultScanner results = table.getScanner(s);
			int i = 0;
			for (Result r : results) {
				LOGGER.log(Level.INFO, "result "+ Bytes.toString(r.getRow()));
				i++;
			}
			Assert.assertEquals("2 order books expected", obs.size(), i);
			
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "IO Exception on testSendAgent", e);
		}
	}

	@Test
	public void testSendExec() {
		// Order o
		System.out.println("HERE!!!!");
	}

}
