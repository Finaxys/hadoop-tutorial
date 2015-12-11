package fr.finaxys.tutorials.utils.spark.analysis;

import fr.finaxys.tutorials.utils.HadoopTutorialException;
import fr.finaxys.tutorials.utils.InjectorTests;
import fr.finaxys.tutorials.utils.TimeStampBuilder;
import fr.finaxys.tutorials.utils.hbase.*;
import fr.univlille1.atom.trace.TraceType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.experimental.categories.Category;
import v13.LimitOrder;
import v13.Order;
import v13.PriceRecord;
import v13.agents.Agent;
import v13.agents.DumbAgent;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;

import static fr.finaxys.tutorials.utils.hbase.AtomHBaseHelper.AGENT;
import static fr.finaxys.tutorials.utils.hbase.AtomHBaseHelper.ORDER;

/**
 * Created by finaxys on 12/11/15.
 */
@Category(InjectorTests.class)
public class HBaseAnalysisTest {

    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(HBaseAnalysisTest.class.getName());

    private static final TableName TEST_TABLE = TableName
            .valueOf("trace");

    private static final byte[] TEST_FAMILY = Bytes.toBytes("cf");


    private static HBaseTestingUtility TEST_UTIL = null;
    private static Configuration CONF = null;
    private static Table table = null;
    private static Table tbAgentPosition = null;

    final HBaseDataTypeEncoder hbEncoder = new HBaseDataTypeEncoder();

    private HBaseAnalysis analysis = new HBaseAnalysis();
    private TimeStampBuilder tsb = null;
    private Date testDate;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        TEST_UTIL = new HBaseTestingUtility();
        CONF = TEST_UTIL.getConfiguration();
//		OutputStream os = new FileOutputStream("/tmp/configuration.xml");
//		CONF.writeXml(os);
//        os.close();
//		Properties prop = new Properties();
//		InputStream in =  HBaseAnalysisTest.class.getResourceAsStream("configuration.xml");
//		prop.load(in);
//		in.close();
//		CONF.addResource(in);
//		CONF.reloadConfiguration();
        //Configuration conf = TEST_UTIL.getConfiguration();
		/*MiniMRCluster mapReduceCluster = *///TEST_UTIL.startMiniMapReduceCluster();
		/*MiniHBaseCluster hbaseCluster = */TEST_UTIL.startMiniCluster();
        table = TEST_UTIL.createTable(TEST_TABLE, new byte[][] { TEST_FAMILY });
        tbAgentPosition = TEST_UTIL.createTable(Bytes.toBytes(AgentPosition.AP_RESULT_TABLE), new byte[][] { Bytes.toBytes(AgentPosition.AP_RESULT_CF) });
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
        //TEST_UTIL.shutdownMiniMapReduceCluster();
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
            analysis.openTable();
        } catch (ParseException e) {
            LOGGER.log(Level.SEVERE, "Could not parse Date for Test:"+sDate+", expected format"+TimeStampBuilder.DATE_FORMAT);
            throw new HadoopTutorialException("Could not init test", e);
        }
    }

    @After
    public void tearDown() {
        try {
            tbAgentPosition.close();
            analysis.closeTable();
            analysis = null;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Could not parse Date for Test", e.getMessage());
            throw new HadoopTutorialException("Could not close table", e);
        }
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
        analysis.directPutTable(p);
        Map<TraceType, Integer> r = analysis.traceCount(testDate, null);
        Assert.assertEquals("1 results return", r.size(), 1);
        Assert.assertEquals("2 should have count 1", r.get(TraceType.Agent), new Integer(1));

        long ts2 = System.currentTimeMillis();
        LimitOrder o2 = new LimitOrder("o", "2", LimitOrder.ASK, 1, 10);
        o2.sender = new DumbAgent("a2");
        o2.timestamp = ts2;


        LOGGER.log(Level.INFO, "Order 2 TimeStamp:"+ts2);
        Put pOrder = analysis.mkPutOrder(analysis.createRequired(ORDER), ts2, o2);
        analysis.directPutTable(pOrder);
        r = analysis.traceCount(testDate, null);
        Assert.assertEquals("1 result should be return", r.size(), 1);
        Assert.assertEquals("2 should have count 1", r.get(TraceType.Agent), new Integer(1));
        Assert.assertEquals("Order should be null", r.get(TraceType.Order), null);
    }
/*
    @SuppressWarnings("unchecked")
    @Test
    public void testZAgentPosition() throws Exception {

        long ts = tsb.nextTimeStamp();

        Agent a = new DumbAgent("a1");
        Order o = new LimitOrder("o", "1", LimitOrder.ASK, 10, 10);
        o.sender = a;
        o.timestamp = ts;

        LOGGER.log(Level.INFO, "Order 1 TimeStamp:"+o.timestamp);
        //Put p = analysis.mkPutOrder(analysis.createRequired(AGENT), ts, a, o , pr);
        Put pOrder = analysis.mkPutOrder(analysis.createRequired(ORDER), ts, o);
        analysis.directPutTable(pOrder);

        LimitOrder o2 = new LimitOrder("o", "2", LimitOrder.ASK, 1, 11);
        o2.sender = a;
        o2.timestamp = ts;

        LOGGER.log(Level.INFO, "Order 2 TimeStamp:"+o2.timestamp);
        Put pOrder2 = analysis.mkPutOrder(analysis.createRequired(ORDER), ts, o2);
        analysis.directPutTable(pOrder2);

        long ts2 = System.currentTimeMillis();
        LimitOrder o3 = new LimitOrder("o", "3", LimitOrder.ASK, 1, 1);
        o3.sender = a;
        o3.timestamp = ts2;

        LOGGER.log(Level.INFO, "Order 3 TimeStamp:"+o3.timestamp);
        Put pOrder3 = analysis.mkPutOrder(analysis.createRequired(ORDER), ts2, o3);
        analysis.directPutTable(pOrder3);

        Map<String, Integer> r = analysis.agentPosition(testDate);
        Assert.assertEquals("1 results return", r.size(), 1);
        Assert.assertEquals("Position should have count 11", new Integer(11), r.get("a1-o"));

//		 TraceCountMap map = new TraceCountMap();
//
//		ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes("test"));
//	    Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context ctx =
//	        mock(Context.class);
//	    when(ctx.getConfiguration()).thenReturn(CONF);
//	    doAnswer(new Answer<Void>() {
//
//	      @Override
//	      public Void answer(InvocationOnMock invocation) throws Throwable {
//	        ImmutableBytesWritable writer = (ImmutableBytesWritable) invocation.getArguments()[0];
//	        Put put = (Put) invocation.getArguments()[1];
//	        assertEquals("tableName-column1", Bytes.toString(writer.get()));
//	        assertEquals("test", Bytes.toString(put.getRow()));
//	        return null;
//	      }
//	    }).when(ctx).write(any(ImmutableBytesWritable.class), any(Put.class));
//	    Result result = mock(Result.class);
//	    when(result.getValue(Bytes.toBytes("columnFamily"), Bytes.toBytes("column1"))).thenReturn(
//	        Bytes.toBytes("test"));
//
//	    map.setup(ctx);
//	    map.map(rowKey, result, ctx);
    }*/
}
