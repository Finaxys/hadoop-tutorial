package fr.finaxys.tutorials.utils.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import v13.Day;
import v13.LimitOrder;
import v13.Order;
import v13.OrderBook;
import v13.PriceRecord;
import v13.agents.Agent;
import v13.agents.DumbAgent;
import fr.finaxys.tutorials.utils.InjectorTests;
import fr.finaxys.tutorials.utils.TimeStampBuilder;
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
			byte[] agentName = result.getValue(TEST_FAMILY, Q_AGENT_NAME);
			byte[] agentName2 = hbEncoder.encodeString(a.name);
			Assert.assertTrue("AgentName is same", Bytes.equals(agentName, agentName2));
			byte[] orderBookName = result.getValue(TEST_FAMILY, Q_OB_NAME);
			byte[] orderBookName2 = hbEncoder.encodeString(o.obName);
			Assert.assertTrue("orderBookName is same", Bytes.equals(orderBookName, orderBookName2));
			byte[] executedQuantity = result.getValue(TEST_FAMILY, Q_EXECUTED_QUANTITY);
			byte[] executedQuantity2 = hbEncoder.encodeInt(pr.quantity);
			Assert.assertTrue("executedQuantity is same", Bytes.equals(executedQuantity, executedQuantity2));
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
			byte[] orderBookName = result.getValue(TEST_FAMILY, Q_OB_NAME);
			byte[] orderBookName2 = hbEncoder.encodeString(pr.obName);
			Assert.assertTrue("orderBookName is same", Bytes.equals(orderBookName, orderBookName2));
			byte[] bestAsk = result.getValue(TEST_FAMILY, Q_BEST_ASK);
			byte[] bestAsk2 = hbEncoder.encodeLong(bestAskPrice);
			Assert.assertTrue("executedQuantity is same", Bytes.equals(bestAsk, bestAsk2));
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
			byte[] dir = result.getValue(TEST_FAMILY, Q_DIRECTION);
			byte[] dir2 = hbEncoder.encodeChar(o.direction);
			Assert.assertTrue("orderBookName is same", Bytes.equals(dir, dir2));
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
			byte[] orderBookName = result.getValue(TEST_FAMILY, Q_OB_NAME);
			byte[] orderBookName2 = hbEncoder.encodeString(ob2.obName);
			Assert.assertTrue("orderBookName is same", Bytes.equals(orderBookName, orderBookName2));
			byte[] dayGap = result.getValue(TEST_FAMILY, EXT_NUM_DAY);
			byte[] dayGap2 = hbEncoder.encodeInt(day.number + DAY_GAP);
			Assert.assertTrue("Day Gap is same", Bytes.equals(dayGap, dayGap2));
			
			byte[] tick = result.getValue(TEST_FAMILY, Q_NUM_TICK);
			byte[] tick2 = hbEncoder.encodeInt(day.currentTick());
			Assert.assertTrue("Tick is same", Bytes.equals(tick, tick2));
			
			
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
			byte[] orderBookName = result.getValue(TEST_FAMILY, Q_OB_NAME);
			byte[] orderBookName2 = hbEncoder.encodeString(ob2.obName);
			Assert.assertTrue("orderBookName is same", Bytes.equals(orderBookName, orderBookName2));
			byte[] dayGap = result.getValue(TEST_FAMILY, EXT_NUM_DAY);
			byte[] dayGap2 = hbEncoder.encodeInt(day + DAY_GAP);
			Assert.assertTrue("Day Gap is same", Bytes.equals(dayGap, dayGap2));
			
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
