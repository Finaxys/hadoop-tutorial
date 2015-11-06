package fr.finaxys.tutorials.utils.hbase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import v13.Day;
import v13.LimitOrder;
import v13.Order;
import v13.OrderBook;
import v13.PriceRecord;
import v13.agents.Agent;

import com.sun.istack.NotNull;

import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.AtomDataInjector;
import fr.finaxys.tutorials.utils.HadoopTutorialException;
import fr.finaxys.tutorials.utils.TimeStampBuilder;

/**
 *
 */
public class SimpleHBaseInjector implements AtomDataInjector {

	public static final String Q_NUM_DAY = "NumDay";

	public static final String Q_NUM_TICK = "NumTick";

	public static final String Q_BEST_BID = "BestBid";

	public static final String Q_BEST_ASK = "BestAsk";

	public static final String Q_ORDER2 = "Order2";

	public static final String Q_ORDER1 = "Order1";

	public static final String Q_DIR = "Dir";

	public static final String Q_OB_NAME = "ObName";

	public static final String Q_VALIDITY = "Validity";

	public static final String Q_QUANTITY = "Quantity";

	public static final String Q_ID = "Id";

	public static final String Q_TYPE = "Type";

	public static final String Q_EXT_ID = "ExtId";

	public static final String Q_SENDER = "Sender";

	public static final String Q_NB_PRICES_FIXED = "NbPricesFixed";

	public static final String Q_LAST_FIXED_PRICE = "LastFixedPrice";

	public static final String Q_HIGHEST_PRICE = "HighestPrice";

	public static final String Q_LOWEST_PRICE = "LowestPrice";

	public static final String Q_FIRST_FIXED_PRICE = "FirstFixedPrice";

	public static final String EXT_NUM_DAY = "NumDay";

	public static final String Q_EXT_ORDER_ID = "OrderExtId";

	public static final String Q_TIMESTAMP = "Timestamp";

	public static final String Q_DIRECTION = "Direction";

	public static final String Q_PRICE = "Price";

	public static final String Q_EXECUTED_QUANTITY = "Executed";

	public static final String Q_CASH = "Cash";

	//public static final String Q_ORDER_BOOK_NAME = "OrderBookName";

	public static final String Q_AGENT_NAME = "AgentName";

	static final String TICK = "T";

	static final String PRICE = "P";

	static final String ORDER = "O";

	static final String EXEC = "E";

	static final String DAY = "D";

	static final String AGENT = "A";

	private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
			.getLogger(SimpleHBaseInjector.class.getName());

	// Confs
	private AtomConfiguration atomConfiguration;
	private Configuration hbaseConfiguration;
	private TimeStampBuilder timeStampBuilder;

	final HBaseDataTypeEncoder hbEncoder = new HBaseDataTypeEncoder();
	
	// Props
	private byte[] columnFamily;
	private TableName tableName;
	private Table table;
	private int dayGap;

	private final AtomicLong globalCount = new AtomicLong(0L);
	private AtomicLong idGen = new AtomicLong(1_000_000);

	public SimpleHBaseInjector(@NotNull AtomConfiguration conf) {
		this.atomConfiguration = conf;
		this.columnFamily = conf.getColumnFamily();
		this.hbaseConfiguration = createHbaseConfiguration();
		this.tableName = TableName.valueOf(atomConfiguration.getTableName());
		this.dayGap = atomConfiguration.getDayGap();
		/* this.table = createHTableConnexion(tableName
				, this.hbaseConfiguration); */
	}
	
	public SimpleHBaseInjector() {
		
	}

	@Override
	public void close() {
		try {
			table.close();
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "IO Exception while closing table", e);
		} finally {
			LOGGER.info("Total put sent : " + globalCount.get());
		}
	}

	private Configuration createHbaseConfiguration() {
		Configuration conf = HBaseConfiguration.create();
		// try {
		// conf.addResource(new
		// File(atomConf.getHadoopConfCore()).getAbsoluteFile().toURI().toURL());
		// conf.addResource(new
		// File(atomConf.getHbaseConfHbase()).getAbsoluteFile().toURI().toURL());
		// conf.addResource(new
		// File(atomConf.getHadoopConfHdfs()).getAbsoluteFile().toURI().toURL());
		// } catch (MalformedURLException e) {
		// LOGGER.log(Level.SEVERE, "Could not get hbase configuration files",
		// e);
		// throw new Exception("hbase", e);
		// }
		// conf.reloadConfiguration();
		return conf;
	}

	private Table createHTableConnexion(TableName tableName,
			Configuration hbConf) throws IOException {
		Connection connection = ConnectionFactory.createConnection(hbConf);
		Table table = connection.getTable(tableName);
		return table;
		
	}

	@Override
	public void createOutput() {
		assert !(tableName == null);
		String port = hbaseConfiguration.get("hbase.zookeeper.property.clientPort");
		String host = hbaseConfiguration.get("hbase.zookeeper.quorum");

		LOGGER.log(Level.INFO, "Try to connect to " + host + ":" + port);

		LOGGER.log(Level.INFO, "Configuration completed");
		
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(hbaseConfiguration);
			createTable(connection);
			this.table = createHTableConnexion(tableName, hbaseConfiguration);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Could not create Connection", e);
			throw new HadoopTutorialException("hbase connection", e);
		} finally {
			if (connection != null)
				try {
					connection.close();
				} catch (IOException e) {
					// can't do anything here...
				}
		}
	}

	@NotNull
	private String createRequired(@NotNull String name) {
		long rowKey = Long.reverseBytes(idGen.incrementAndGet());
		return String.valueOf(rowKey) + name;
	}
	
	/**
	 * Absolutely not thread safe. Only used for Unit test
	 * @param name
	 * @return
	 */
	protected String getLastRequired(@NotNull String name) {
		long rowKey = Long.reverseBytes(idGen.get());
		return String.valueOf(rowKey) + name;
	}

	private void createTable(Connection connection) {
		Admin admin = null;
		try {
			admin = connection.getAdmin();
		} catch (MasterNotRunningException e) {
			LOGGER.log(Level.SEVERE, "Master server not running", e);
			throw new HadoopTutorialException("hbase master server", e);
		} catch (ZooKeeperConnectionException e) {
			LOGGER.log(Level.SEVERE, "Could not connect to ZooKeeper", e);
			throw new HadoopTutorialException("zookeeper", e);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "IOException while accessing hbase admin",
					e);
			throw new HadoopTutorialException("ioexception getAdmin", e);
		}

		try {
			if (admin.tableExists(tableName)) {
				LOGGER.log(Level.INFO, tableName
						+ " already exists");
				return;
			}
		} catch (IOException e1) {
			LOGGER.log(Level.SEVERE, "IOException while checking table exist",
					e1);
			throw new HadoopTutorialException("ioexception table exists", e1);
		}

		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
		try {
			LOGGER.log(Level.INFO, "Creating table");
			LOGGER.log(Level.INFO, admin.getClusterStatus().toString());

			tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDescriptor);
			LOGGER.log(Level.INFO, "Table Created");
		} catch (IOException e) // ajouter exception spécique à la non création
								// de table
		{
			LOGGER.log(Level.SEVERE,
					"Table already created but trying to create it!");
			throw new HadoopTutorialException("Cannot create table (should be a zombie)", e);
		}
	}

	public AtomConfiguration getAtomConfiguration() {
		return atomConfiguration;
	}

	public byte[] getColumnFamily() {
		return columnFamily;
	}

	public Configuration getHbaseConfiguration() {
		return hbaseConfiguration;
	}

	public Table getTable() {
		return table;
	}

	public TimeStampBuilder getTimeStampBuilder() {
		return timeStampBuilder;
	}
	
	@Override
	public void setTimeStampBuilder(TimeStampBuilder tsb) {
		this.timeStampBuilder = tsb;
	}

	private void putTable(@NotNull Put p) {
		try {
			table.put(p);
			globalCount.addAndGet(1);
		} catch (IOException e) {
			LOGGER.severe("Failed to push data into queue : " + e.getMessage());
			throw new HadoopTutorialException("Failed to push data into queue", e);
		}
	}

	@Override
	public void sendAgent(Agent a, Order o, PriceRecord pr) {
		Put p = new Put(Bytes.toBytes(createRequired(AGENT)));
		p.addColumn(columnFamily, Bytes.toBytes(Q_AGENT_NAME),
				hbEncoder.encodeString(a.name));
		p.addColumn(columnFamily, Bytes.toBytes(Q_OB_NAME),
				hbEncoder.encodeString(o.obName));
		p.addColumn(columnFamily, Bytes.toBytes(Q_CASH), hbEncoder.encodeLong(a.cash));
		p.addColumn(columnFamily, Bytes.toBytes(Q_EXECUTED_QUANTITY),
				hbEncoder.encodeInt(pr.quantity));
		p.addColumn(columnFamily, Bytes.toBytes(Q_PRICE),
				hbEncoder.encodeLong(pr.price));
		if (o.getClass().equals(LimitOrder.class)) {
			p.addColumn(columnFamily, Bytes.toBytes(Q_DIRECTION),
					hbEncoder.encodeChar(((LimitOrder) o).direction));
			p.addColumn(columnFamily, Bytes.toBytes(Q_TIMESTAMP),
					hbEncoder.encodeLong(pr.timestamp)); // pr.timestamp
			p.addColumn(columnFamily, Bytes.toBytes(Q_EXT_ORDER_ID),
					hbEncoder.encodeString(o.extId));
		}
		putTable(p);
	}

	@Override
	public void sendAgentReferential(List<AgentReferentialLine> referencial) {
		Table table = null;
		try {
			table = createHTableConnexion(tableName, hbaseConfiguration);
			for (AgentReferentialLine agent : referencial) {
				Put p = agent.toPut(hbEncoder, columnFamily,
						System.currentTimeMillis());
				table.put(p);
			}
			table.close();
		} catch (IOException e) {
			// @TODO change message + LOG
			throw new HadoopTutorialException("Failed to push data into queue", e);
		}
	}

	@Override
	public void sendDay(int nbDays, Collection<OrderBook> orderbooks) {
		
		for (OrderBook ob : orderbooks) {
			Put p = new Put(Bytes.toBytes(createRequired(DAY)));
			p.addColumn(columnFamily, Bytes.toBytes(EXT_NUM_DAY),
					hbEncoder.encodeInt(nbDays + dayGap));
			p.addColumn(columnFamily, Bytes.toBytes(Q_OB_NAME),
					hbEncoder.encodeString(ob.obName));
			p.addColumn(columnFamily, Bytes.toBytes(Q_FIRST_FIXED_PRICE),
					hbEncoder.encodeLong(ob.firstPriceOfDay));
			p.addColumn(columnFamily, Bytes.toBytes(Q_LOWEST_PRICE),
					hbEncoder.encodeLong(ob.lowestPriceOfDay));
			p.addColumn(columnFamily, Bytes.toBytes(Q_HIGHEST_PRICE),
					hbEncoder.encodeLong(ob.highestPriceOfDay));
			long price = 0;
			if (ob.lastFixedPrice != null)
				price = ob.lastFixedPrice.price;
			p.addColumn(columnFamily, Bytes.toBytes(Q_LAST_FIXED_PRICE),
					hbEncoder.encodeLong(price));
			p.addColumn(columnFamily, Bytes.toBytes(Q_NB_PRICES_FIXED),
					hbEncoder.encodeLong(ob.numberOfPricesFixed));
			putTable(p);
		}
	}



	@Override
	public void sendExec(Order o) {
		Put p = new Put(Bytes.toBytes(createRequired(EXEC)));
		p.addColumn(columnFamily, Bytes.toBytes(Q_SENDER),
				hbEncoder.encodeString(o.sender.name));
		p.addColumn(columnFamily, Bytes.toBytes(Q_EXT_ID),
				hbEncoder.encodeString(o.extId));
		putTable(p);
	}



	@Override
	public void sendOrder(Order o) {
		o.timestamp = timeStampBuilder.nextTimeStamp();
		long ts = System.currentTimeMillis(); // hack for update on scaledrisk
												// (does not manage put then
												// update with same ts)
		Put p = new Put(Bytes.toBytes(createRequired(ORDER)), ts);
		p.addColumn(columnFamily, Bytes.toBytes(Q_OB_NAME),
				hbEncoder.encodeString(o.obName)); 
		p.addColumn(columnFamily, Bytes.toBytes(Q_SENDER),
				hbEncoder.encodeString(o.sender.name));
		p.addColumn(columnFamily, Bytes.toBytes(Q_EXT_ID), hbEncoder.encodeString(o.extId)); 
		p.addColumn(columnFamily, Bytes.toBytes(Q_TYPE), hbEncoder.encodeChar(o.type));
		p.addColumn(columnFamily, Bytes.toBytes(Q_ID), hbEncoder.encodeLong(o.id));
		p.addColumn(columnFamily, Bytes.toBytes(Q_TIMESTAMP),
				hbEncoder.encodeLong(o.timestamp)); // o.timestamp

		// Date d = new Date(tsb.getTimeStamp());
		if (o.getClass().equals(LimitOrder.class)) {
			LimitOrder lo = (LimitOrder) o;
			p.addColumn(columnFamily, Bytes.toBytes(Q_QUANTITY),
					hbEncoder.encodeInt(lo.quantity));
			p.addColumn(columnFamily, Bytes.toBytes(Q_DIRECTION),
					hbEncoder.encodeChar(lo.direction));
			p.addColumn(columnFamily, Bytes.toBytes(Q_PRICE),
					hbEncoder.encodeLong(lo.price));
			p.addColumn(columnFamily, Bytes.toBytes(Q_VALIDITY),
					hbEncoder.encodeLong(lo.validity));
		}
		putTable(p);
	}



	@Override
	public void sendPriceRecord(PriceRecord pr, long bestAskPrice,
			long bestBidPrice) {
		long ts = System.currentTimeMillis() + 2L; // hack for update on
													// scaledrisk (does not
													// manage put then update
													// with same ts)
		pr.timestamp = timeStampBuilder.nextTimeStamp();

		Put p = new Put(Bytes.toBytes(createRequired(PRICE)), ts);
		p.addColumn(columnFamily, Bytes.toBytes(Q_OB_NAME), ts,
				hbEncoder.encodeString(pr.obName));
		p.addColumn(columnFamily, Bytes.toBytes(Q_PRICE), ts,
				hbEncoder.encodeLong(pr.price));
		p.addColumn(columnFamily, Bytes.toBytes(Q_EXECUTED_QUANTITY), ts,
				hbEncoder.encodeInt(pr.quantity));
		p.addColumn(columnFamily, Bytes.toBytes(Q_DIR), ts,
				hbEncoder.encodeChar(pr.dir));
		p.addColumn(columnFamily, Bytes.toBytes(Q_ORDER1), ts,
				hbEncoder.encodeString(pr.extId1));
		p.addColumn(columnFamily, Bytes.toBytes(Q_ORDER2), ts,
				hbEncoder.encodeString(pr.extId2));
		p.addColumn(columnFamily, Bytes.toBytes(Q_BEST_ASK), ts,
				hbEncoder.encodeLong(bestAskPrice));
		p.addColumn(columnFamily, Bytes.toBytes(Q_BEST_BID), ts,
				hbEncoder.encodeLong(bestBidPrice));
		p.addColumn(columnFamily, Bytes.toBytes(Q_TIMESTAMP), ts,
				hbEncoder.encodeLong((pr.timestamp > 0 ? pr.timestamp : ts))); // tsb.nextTimeStamp()

		putTable(p);
	}



	@Override
	public void sendTick(Day day, Collection<OrderBook> orderbooks) {
		for (OrderBook ob : orderbooks) {

			Put p = new Put(Bytes.toBytes(createRequired(TICK)));
			p.addColumn(columnFamily, Bytes.toBytes(Q_NUM_TICK),
					hbEncoder.encodeInt(day.currentTick()));
			p.addColumn(columnFamily, Bytes.toBytes(Q_NUM_DAY),
					hbEncoder.encodeInt(day.number + dayGap));
			p.addColumn(columnFamily, Bytes.toBytes(Q_OB_NAME),
					hbEncoder.encodeString(ob.obName));
			if (!ob.ask.isEmpty())
				p.addColumn(columnFamily, Bytes.toBytes(Q_BEST_ASK),
						hbEncoder.encodeLong(ob.ask.last().price));

			if (!ob.bid.isEmpty())
				p.addColumn(columnFamily, Bytes.toBytes(Q_BEST_BID),
						hbEncoder.encodeLong(ob.bid.last().price));

			if (ob.lastFixedPrice != null)
				p.addColumn(columnFamily, Bytes.toBytes(Q_LAST_FIXED_PRICE),
						hbEncoder.encodeLong(ob.lastFixedPrice.price));

			putTable(p);
		}
	}

	public void setAtomConfiguration(AtomConfiguration atomConfiguration) {
		this.atomConfiguration = atomConfiguration;
	}

	public void setHbaseConfiguration(Configuration hbaseConfiguration) {
		this.hbaseConfiguration = hbaseConfiguration;
	}

	public void setTableName(TableName tableName) {
		this.tableName = tableName;
	}

	public void setColumnFamily(byte[] columnFamily) {
		this.columnFamily = columnFamily;
	}

	public int getDayGap() {
		return dayGap;
	}

	public void setDayGap(int dayGap) {
		this.dayGap = dayGap;
	}
}
