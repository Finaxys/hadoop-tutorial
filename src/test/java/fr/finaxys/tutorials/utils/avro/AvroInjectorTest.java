package fr.finaxys.tutorials.utils.avro;

import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.InjectorTests;
import fr.finaxys.tutorials.utils.TimeStampBuilder;
import fr.finaxys.tutorials.utils.avro.models.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import v13.*;
import v13.Day;
import v13.Order;
import v13.agents.Agent;
import v13.agents.DumbAgent;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by finaxys on 11/19/15.
 */
@Category(InjectorTests.class)
public class AvroInjectorTest {

    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(AvroInjector.class.getName());
    private static final int DAY_GAP = 2;
    private static HBaseTestingUtility TEST_UTIL = null;
    private static Configuration CONF = null;
    private static AvroInjector injector ;
    private static AvroReader reader ;
    private TimeStampBuilder tsb = null;
    private static AtomConfiguration ATOM_CONF ;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        TEST_UTIL = new HBaseTestingUtility();
        CONF = TEST_UTIL.getConfiguration();
        TEST_UTIL.startMiniCluster();
        Configuration conf = TEST_UTIL.getConfiguration() ;
        ATOM_CONF = new AtomConfiguration() ;
        injector = new AvroInjector(ATOM_CONF,conf) ;
        reader = new AvroReader(ATOM_CONF,conf);
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
        injector.setDayGap(DAY_GAP);
    }

    @After
    public void tearDown() {
        injector.closeOutput();
    }

    @Test
    public void testSendAgent() {
        // Agent a, Order o, PriceRecord pr
            injector.createOutput();
            Agent a = new DumbAgent("a");
            Order o = new LimitOrder("o", "1", LimitOrder.ASK, 1, 10);
            o.sender = a;
            PriceRecord pr = new PriceRecord("o", 10, 1, LimitOrder.ASK, "o-1", "o-2");
            injector.sendAgent(a, o, pr);
            injector.closeOutput();
            fr.finaxys.tutorials.utils.avro.models.Agent agent = reader.scanAgents().get(0);
            String agentName = agent.getAgentName().toString();
            Assert.assertTrue("AgentName is same", agentName.equals(a.name));
            String orderBookName = agent.getObName().toString() ;
            Assert.assertTrue("orderBookName is same", orderBookName.equals(o.obName));
            int executedQuantity = agent.getExecuted() ;
            Assert.assertTrue("executedQuantity is same", executedQuantity == pr.quantity);

    }


    @Test
    public void testSendPriceRecord() {
        // PriceRecord pr, long bestAskPrice, long bestBidPrice
            injector.createOutput();
            PriceRecord pr = new PriceRecord("pr", 10, 1, LimitOrder.ASK, "o-1", "o-2");
            long bestAskPrice = 1;
            long bestBidPrice = 2;
            injector.sendPriceRecord(pr, bestAskPrice, bestBidPrice);
            injector.closeOutput();
            fr.finaxys.tutorials.utils.avro.models.Price price = reader.scanPrices().get(0);
            String orderBookName = price.getObName().toString();
            Assert.assertTrue("orderBookName is same", orderBookName.equals( pr.obName));
            long bestAsk = price.getBestAsk();
            Assert.assertTrue("executedQuantity is same", bestAsk==bestAskPrice);
    }

    @Test
    public void testSendAgentReferential() {
        // List<AgentReferentialLine> referencial
        System.out.println("HERE!!!!");
    }

    @Test
    public void testSendOrder() {
        // Order o
            injector.createOutput();
            LimitOrder o = new LimitOrder("o", "1", LimitOrder.ASK, 1, 10);
            o.sender = new DumbAgent("a");
            injector.sendOrder(o);
            injector.closeOutput();
            fr.finaxys.tutorials.utils.avro.models.Order order =  reader.scanOrders().get(0);
            String orderBookName = order.getObName().toString();
            String orderBookName2 = o.obName;
            Assert.assertTrue("orderBookName is same", orderBookName.equals(orderBookName2));
            char dir = order.getDirection().charAt(0);
            Assert.assertTrue("orderBookName is same", dir == o.direction);
    }

    @Test
    public void testSendTick() {
        // Day day, Collection<OrderBook> orderbooks
            injector.createOutput();
            Day day = Day.createSinglePeriod(1, 100);
            day.nextPeriod();
            OrderBook ob = new OrderBook("ob1");
            OrderBook ob2 = new OrderBook("ob2");
            List<OrderBook> obs = new ArrayList<OrderBook>();
            obs.add(ob);
            obs.add(ob2);
            injector.sendTick(day, obs);
            injector.closeOutput();
            Tick tick = reader.scanTicks().get(1);
            String orderBookName = tick.getObName().toString();
            Assert.assertTrue("orderBookName is same", orderBookName.equals(ob2.obName));
            int dayGap = tick.getNumDay();
            Assert.assertTrue("Day Gap is same", dayGap ==  day.number + DAY_GAP);
            int num = tick.getNumTick();
            Assert.assertTrue("Tick is same", num == day.currentTick());
    }

    @Test
    public void testSendDay() {
        // int nbDays, Collection<OrderBook> orderbooks
            injector.createOutput();
            int day = 1;
            OrderBook ob = new OrderBook("ob1");
            OrderBook ob2 = new OrderBook("ob2");
            List<OrderBook> obs = new ArrayList<OrderBook>();
            obs.add(ob);
            obs.add(ob2);
            injector.sendDay(day, obs);
            injector.closeOutput();
            fr.finaxys.tutorials.utils.avro.models.Day avroDay = reader.scanDays().get(1);
            String orderBookName = avroDay.getObName().toString() ;
            Assert.assertTrue("orderBookName is same", orderBookName.equals(ob2.obName));
            int dayGap = avroDay.getNumDay();
            Assert.assertTrue("Day Gap is same", dayGap == day + DAY_GAP);
    }

    @Test
    public void testSendExec() {
        // Order o
        System.out.println("HERE!!!!");
    }
}
