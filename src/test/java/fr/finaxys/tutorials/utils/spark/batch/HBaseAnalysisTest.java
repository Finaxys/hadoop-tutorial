package fr.finaxys.tutorials.utils.spark.batch;

import fr.finaxys.tutorials.utils.HadoopTutorialException;
import fr.finaxys.tutorials.utils.InjectorTests;
import fr.finaxys.tutorials.utils.TimeStampBuilder;
import fr.finaxys.tutorials.utils.hbase.AgentPosition;
import fr.finaxys.tutorials.utils.hbase.HBaseDataTypeEncoder;
import fr.finaxys.tutorials.utils.hbase.SimpleHBaseInjector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
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
import java.util.logging.Level;


/**
 * Created by finaxys on 12/10/15.
 */
@Category(InjectorTests.class)
public class HBaseAnalysisTest {

    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(HBaseAnalysisTest.class.getName());

    private static final TableName TEST_TABLE = TableName
            .valueOf("testhbaseinjector");

    private static final byte[] TEST_FAMILY = Bytes.toBytes("cf");


    private static HBaseTestingUtility TEST_UTIL = null;
    private static Configuration CONF = null;
    private static Table table = null;
    private static Table tbAgentPosition = null;

    final HBaseDataTypeEncoder hbEncoder = new HBaseDataTypeEncoder();
    private SimpleHBaseInjector injector = new SimpleHBaseInjector();

    private fr.finaxys.tutorials.utils.spark.batch.HBaseAnalysis analysis = new fr.finaxys.tutorials.utils.spark.batch.HBaseAnalysis();
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
		/*MiniHBaseCluster hbaseCluster = */
        TEST_UTIL.startMiniCluster();
        table = TEST_UTIL.createTable(TEST_TABLE, new byte[][]{TEST_FAMILY});
        tbAgentPosition = TEST_UTIL.createTable(Bytes.toBytes(AgentPosition.AP_RESULT_TABLE), new byte[][]{Bytes.toBytes(AgentPosition.AP_RESULT_CF)});
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
        } catch (ParseException e) {
            LOGGER.log(Level.SEVERE, "Could not parse Date for Test:" + sDate + ", expected format" + TimeStampBuilder.DATE_FORMAT);
            throw new HadoopTutorialException("Could not init test", e);
        }
    }

    @After
    public void tearDown() {
        try {
            tbAgentPosition.close();
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

/*        LOGGER.log(Level.INFO, "Order 1 TimeStamp:" + ts);
        Put p = analysis.mkPutAgent(analysis.createRequired(AGENT), ts, a, o, pr);
        analysis.directPutTable(p);
        Map<TraceType, Integer> r = analysis.traceCount(testDate, null);
        Assert.assertEquals("1 results return", r.size(), 1);
        Assert.assertEquals("2 should have count 1", r.get(TraceType.Agent), new Integer(1));

        long ts2 = System.currentTimeMillis();
        LimitOrder o2 = new LimitOrder("o", "2", LimitOrder.ASK, 1, 10);
        o2.sender = new DumbAgent("a2");
        o2.timestamp = ts2;

        LOGGER.log(Level.INFO, "Order 2 TimeStamp:" + ts2);
        Put pOrder = analysis.mkPutOrder(analysis.createRequired(ORDER), ts2, o2);
        analysis.directPutTable(pOrder);
        r = analysis.traceCount(testDate, null);
        Assert.assertEquals("1 result should be return", r.size(), 1);
        Assert.assertEquals("2 should have count 1", r.get(TraceType.Agent), new Integer(1));
        Assert.assertEquals("Order should be null", r.get(TraceType.Order), null);*/
    }
}