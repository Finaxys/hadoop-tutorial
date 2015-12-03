package fr.finaxys.tutorials.utils.parquet;

import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.InjectorTests;
import fr.finaxys.tutorials.utils.avro.AvroInjector;
import fr.finaxys.tutorials.utils.hdfs.HDFSReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.*;
import org.junit.experimental.categories.Category;
import v13.LimitOrder;
import v13.Order;
import v13.PriceRecord;
import v13.agents.Agent;
import v13.agents.DumbAgent;

import java.util.logging.Level;

/**
 * Created by finaxys on 12/2/15.
 */
@Category(InjectorTests.class)
public class AvroParquetConverterTest {
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(AvroParquetConverterTest.class.getName());
    private static HBaseTestingUtility TEST_UTIL = null;
    private static Configuration CONF = null;
    private static AvroParquetConverter converter;
    private static HDFSReader hdfsReader;
    private static ParquetReader parquetReader;
    private static AvroInjector avroInjector;
    private static String finalResultFile = "/final-result";
    private static String resultSuffix = "/part-m-00000" ;
    private static int max = 10 ;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        TEST_UTIL = new HBaseTestingUtility();
        TEST_UTIL.startMiniCluster();
        CONF = TEST_UTIL.getConfiguration();
        AtomConfiguration atomConf = new AtomConfiguration();
        converter = new AvroParquetConverter(atomConf,CONF);
        parquetReader = new ParquetReader(atomConf,CONF);
        avroInjector = new AvroInjector(atomConf,CONF);
        LOGGER.log(Level.INFO, "ready to start tests");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
        LOGGER.log(Level.INFO, "end of tests");
    }

    @Before
    public void setUp() {
        converter.setConfiguration(CONF);
        parquetReader.setConfiguration(CONF);
        avroInjector.createOutput();
        LOGGER.log(Level.INFO, "setup done");
    }

    @After
    public void tearDown() {
        LOGGER.log(Level.INFO, "tear down done");
    }

    @Test
    public void testConvert() {
        // prepare data to convert
        Agent a = new DumbAgent("a");
        Order o = new LimitOrder("o", "1", LimitOrder.ASK, 1, 10);
        o.sender = a;
        PriceRecord pr = new PriceRecord("o", 10, 1, LimitOrder.ASK, "o-1", "o-2");
        avroInjector.sendAgent(a, o, pr);
        avroInjector.closeOutput();
        LOGGER.log(Level.INFO, "data injection done");

        //convert data
        boolean conversion = converter.convert();
        Assert.assertTrue("Conversion done without errors",conversion);
        LOGGER.log(Level.INFO, "Conversion done");

        //read from converted file
        parquetReader.read(finalResultFile);

        String fileValue = null;
        try {
            hdfsReader = new HDFSReader(CONF) ;
            fileValue = hdfsReader.getHDFSFile(finalResultFile+resultSuffix, max) ;
            System.out.print(fileValue);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "can't read file result :"+e.getMessage());
        }
        LOGGER.log(Level.INFO, "Reading data done");
        Assert.assertTrue("result converted as required", "agent;Trace:Agent;AgentName:a;ObName:o;Cash:0;Executed:1;Price:10;Direction:A;Timestamp:0;OrderExtId:1\n".equals(fileValue));

    }



}
