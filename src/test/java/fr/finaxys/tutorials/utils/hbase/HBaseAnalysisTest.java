package fr.finaxys.tutorials.utils.hbase;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import v13.LimitOrder;
import v13.Order;
import v13.PriceRecord;
import v13.agents.Agent;
import v13.agents.DumbAgent;
import fr.finaxys.tutorials.utils.HadoopTutorialException;
import fr.finaxys.tutorials.utils.InjectorTests;
import fr.finaxys.tutorials.utils.TimeStampBuilder;
import fr.univlille1.atom.trace.TraceType;
import static fr.finaxys.tutorials.utils.hbase.AtomHBaseHelper.*;

@Category(InjectorTests.class)
public class HBaseAnalysisTest {
	
	private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
			.getLogger(SimpleHBaseInjectorTest.class.getName());
	
	private static final TableName TEST_TABLE = TableName
			.valueOf("testhbaseinjector");
	
	private static final byte[] TEST_FAMILY = Bytes.toBytes("f");
	

	private static HBaseTestingUtility TEST_UTIL = null;
	private static Configuration CONF = null;
	private static Table table = null;
	
	final HBaseDataTypeEncoder hbEncoder = new HBaseDataTypeEncoder();

	private HBaseAnalysis analysis = new HBaseAnalysis();
	private TimeStampBuilder tsb = null;
	private Date testDate;

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
		String sDate = "09/13/1986";
		try {
			SimpleDateFormat f = new SimpleDateFormat(TimeStampBuilder.DATE_FORMAT);
			testDate = f.parse(sDate);
			tsb = new TimeStampBuilder(sDate, "9:00", "17:30", 3000, 2, 2);
			tsb.init();
			analysis.setHbaseConfiguration(CONF);
			analysis.setTableName(TEST_TABLE);
			analysis.setColumnFamily(TEST_FAMILY);
		} catch (ParseException e) {
			LOGGER.log(Level.SEVERE, "Could not parse Date for Test:"+sDate+", expected format"+TimeStampBuilder.DATE_FORMAT);
			throw new HadoopTutorialException("Could not init test", e);
		}
	}

	@After
	public void tearDown() {
		analysis = null;
	}

	
	@Test
	public void testTraceCount() {
		long ts = tsb.nextTimeStamp();
		
		Agent a = new DumbAgent("a1");
		Order o = new LimitOrder("o", "1", LimitOrder.ASK, 1, 10);
		o.sender = a;
		PriceRecord pr = new PriceRecord("o", 10, 1, LimitOrder.ASK, "o-1", "o-2");
		pr.timestamp = ts;
		
		LOGGER.log(Level.INFO, "Order 1 TimeStamp:"+ts);
		Put p = analysis.mkPutAgent(analysis.createRequired(AGENT), ts, a, o , pr);
		analysis.putTable(p);
		Map<TraceType, Integer> r = analysis.traceCount(testDate, null);
		Assert.assertEquals("1 results return", r.size(), 1);
		Assert.assertEquals("2 should have count 1", r.get(TraceType.Agent), new Integer(1));
		
		long ts2 = System.currentTimeMillis();
		LimitOrder o2 = new LimitOrder("o", "2", LimitOrder.ASK, 1, 10);
		o2.sender = new DumbAgent("a2");
		o2.timestamp = ts2;
		
		LOGGER.log(Level.INFO, "Order 2 TimeStamp:"+ts2);
		Put pOrder = analysis.mkPutOrder(analysis.createRequired(ORDER), ts2, o2);
		analysis.putTable(pOrder);
		r = analysis.traceCount(testDate, null);
		Assert.assertEquals("1 result should be return", r.size(), 1);
		Assert.assertEquals("2 should have count 1", r.get(TraceType.Agent), new Integer(1));
		Assert.assertEquals("Order should be null", r.get(TraceType.Order), null);
	}
}
