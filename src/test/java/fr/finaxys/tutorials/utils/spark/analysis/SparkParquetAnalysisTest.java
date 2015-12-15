package fr.finaxys.tutorials.utils.spark.analysis;

/**
 * Created by finaxys on 12/15/15.
 */

import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.HadoopTutorialException;
import fr.finaxys.tutorials.utils.InjectorTests;
import fr.finaxys.tutorials.utils.TimeStampBuilder;
import fr.finaxys.tutorials.utils.avro.AvroInjector;
import fr.finaxys.tutorials.utils.hbase.HBaseDataTypeEncoder;
import fr.finaxys.tutorials.utils.parquet.AvroParquetConverter;
import fr.univlille1.atom.trace.TraceType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
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

/**
 * Created by finaxys on 12/11/15.
 */
@Category(InjectorTests.class)
public class SparkParquetAnalysisTest {

    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(SparkParquetAnalysisTest.class.getName());





    private static HBaseTestingUtility TEST_UTIL = null;
    private static Configuration CONF = null;
    private static AvroInjector injector ;
    private static AvroParquetConverter converter ;
    private static AtomConfiguration atomConfiguration = new AtomConfiguration();

    final HBaseDataTypeEncoder hbEncoder = new HBaseDataTypeEncoder();

    static private SparkParquetAnalysis analysis ;
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
        CONF = TEST_UTIL.getConfiguration() ;
        analysis = new SparkParquetAnalysis(CONF) ;
        injector = new AvroInjector(atomConfiguration,CONF) ;
        converter = new AvroParquetConverter(atomConfiguration,CONF) ;
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
            LOGGER.log(Level.SEVERE, "Could not parse Date for Test:"+sDate+", expected format"+TimeStampBuilder.DATE_FORMAT);
            throw new HadoopTutorialException("Could not init test", e);
        }
    }

    @After
    public void tearDown() {
        try {
            FileSystem fs = FileSystem.get(CONF);
            fs.delete(new Path(atomConfiguration.getAvroHDFSDest()),true);
            fs.delete(new Path(atomConfiguration.getParquetHDFSDest()),true);
            LOGGER.info("Avro and Parquet files deleted from HDFS");
        } catch (IOException e) {
            LOGGER.severe("can't delete files from HDFS");
            throw new HadoopTutorialException();
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
        injector.createOutput();
        injector.sendAgent(a, o , pr);


        long ts2 = System.currentTimeMillis();
        LimitOrder o2 = new LimitOrder("o", "2", LimitOrder.ASK, 1, 10);
        o2.sender = new DumbAgent("a2");
        o2.timestamp = ts2;


        LOGGER.log(Level.INFO, "Order 2 TimeStamp:"+ts2);
        injector.sendOrder(o2);
        injector.closeOutput();
        converter.convert() ;
        Map<TraceType,Integer>r = analysis.traceCount(testDate, null);
        Assert.assertEquals("1 result should be return", r.size(), 1);
        Assert.assertEquals("2 should have count 1", r.get(TraceType.Agent), new Integer(1));
        Assert.assertEquals("Order should be null", r.get(TraceType.Order), null);
    }
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
        injector.createOutput();
        injector.sendOrder(o);

        LimitOrder o2 = new LimitOrder("o", "2", LimitOrder.ASK, 1, 11);
        o2.sender = a;
        o2.timestamp = ts;

        LOGGER.log(Level.INFO, "Order 2 TimeStamp:"+o2.timestamp);
        injector.sendOrder(o2);


        long ts2 = System.currentTimeMillis();
        LimitOrder o3 = new LimitOrder("o", "3", LimitOrder.ASK, 1, 1);
        o3.sender = a;
        o3.timestamp = ts2;

        LOGGER.log(Level.INFO, "Order 3 TimeStamp:"+o3.timestamp);
        injector.sendOrder(o3);
        injector.closeOutput();
        converter.convert();

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
    }

}
