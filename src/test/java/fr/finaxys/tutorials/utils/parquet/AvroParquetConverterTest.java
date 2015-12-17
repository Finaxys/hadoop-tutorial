package fr.finaxys.tutorials.utils.parquet;

import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.InjectorTests;
import fr.finaxys.tutorials.utils.TimeStampBuilder;
import fr.finaxys.tutorials.utils.avro.AvroInjector;
import fr.finaxys.tutorials.utils.hdfs.HDFSReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.*;
import org.junit.experimental.categories.Category;
import v13.*;
import v13.agents.Agent;
import v13.agents.DumbAgent;

import java.util.ArrayList;
import java.util.List;
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
    private static int max = 1000 ;

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
        TimeStampBuilder tsb = new TimeStampBuilder("09/13/1986", "9:00", "17:30", 3000, 2, 2);
        tsb.init();
        avroInjector.setTimeStampBuilder(tsb);
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
        //put an agent example
        Agent a = new DumbAgent("a");
        Order o = new LimitOrder("o", "1", LimitOrder.ASK, 1, 10);
        o.sender = a;
        PriceRecord pr = new PriceRecord("o", 10, 1, LimitOrder.ASK, "o-1", "o-2");
        avroInjector.sendAgent(a, o, pr);

        //put a price example
        PriceRecord price = new PriceRecord("pr", 10, 1, LimitOrder.ASK, "o-1", "o-2");
        long bestAskPrice = 1;
        long bestBidPrice = 2;
        avroInjector.sendPriceRecord(price, bestAskPrice, bestBidPrice);

        //put order
        LimitOrder order = new LimitOrder("o", "1", LimitOrder.ASK, 1, 10);
        order.sender = new DumbAgent("a");
        avroInjector.sendOrder(order);

        //put a tick example
        Day day = Day.createSinglePeriod(1, 100);
        day.nextPeriod();
        OrderBook ob = new OrderBook("ob1");
        OrderBook ob2 = new OrderBook("ob2");
        List<OrderBook> obs = new ArrayList<OrderBook>();
        obs.add(ob);
        obs.add(ob2);
        avroInjector.sendTick(day, obs);

        //put a day example
        int nbDays = 1;
        OrderBook ob3 = new OrderBook("ob1");
        OrderBook ob4 = new OrderBook("ob2");
        List<OrderBook> obList = new ArrayList<OrderBook>();
        obList.add(ob3);
        obList.add(ob4);
        avroInjector.sendDay(nbDays, obList);

        //close injector
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
            System.out.print("file result : \n"+fileValue);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "can't read file result :"+e.getMessage());
        }

        LOGGER.log(Level.INFO, "Reading data done");

        String convertedText = "type: Agent\n" + "agent\n" + "  Trace: Agent\n" + "  AgentName: a\n" + "  ObName: o\n" + "  Cash: 0\n" + "  Executed: 1\n" + "  Price: 10\n" + "  Direction: A\n" + "  OrderExtId: 1\n" + "Timestamp: 0\n" +
                "\n" +"type: Price\n" +"price\n" +"  Trace: Price\n" +"  ObName: pr\n" +"  Price: 10\n" +"  Executed: 1\n" +"  Order1: o-1\n" +"  Order2: o-2\n" + "  BestAsk: 1\n" + "  BestBid: 2\n" + "  Dir: A\n" + "Timestamp: 526978801275\n" +
                "\n" + "type: Order\n" + "order\n" + "  Trace: Order\n" + "  ObName: o\n" + "  Sender: a\n" + "  ExtId: 1\n" + "  Type: L\n" + "  Id: -1\n" + "  Quantity: 1\n" + "  Direction: A\n" + "  Price: 10\n" + "  Validity: -1\n" + "Timestamp: 0\n" +
                "\n" + "type: Tick\n" + "Timestamp: 526978802550\n" + "tick\n" + "  Trace: Tick\n" + "  NumTick: 0\n" + "  NumDay: 0\n" + "  ObName: ob1\n" + "  BestAsk: 0\n" + "  BestBid: 0\n" + "  LastFixedPrice: 0\n" +
                "\n" + "type: Tick\n" + "Timestamp: 526978802550\n" + "tick\n" + "  Trace: Tick\n" + "  NumTick: 0\n" + "  NumDay: 0\n" + "  ObName: ob2\n" + "  BestAsk: 0\n" + "  BestBid: 0\n" + "  LastFixedPrice: 0\n" +
                "\n" + "type: Day\n" + "day\n" + "  Trace: Day\n" + "  NumDay: 1\n" + "  ObName: ob1\n" + "  FirstFixedPrice: -1\n" + "  LowestPrice: -1\n" + "  HighestPrice: -1\n" + "  LastFixedPrice: 0\n" + "  NbPricesFixed: 0\n" + "Timestamp: 526978803825\n" +
                "\n" + "type: Day\n" + "day\n" + "  Trace: Day\n" + "  NumDay: 1\n" + "  ObName: ob2\n" + "  FirstFixedPrice: -1\n" + "  LowestPrice: -1\n" + "  HighestPrice: -1\n" + "  LastFixedPrice: 0\n" + "  NbPricesFixed: 0\n" + "Timestamp: 526978803825\n" + "\n";

        Assert.assertTrue("result converted as required", convertedText.equals(fileValue));

        LOGGER.log(Level.INFO, "tests done");

    }



}
