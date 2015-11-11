package fr.finaxys.tutorials.utils.hbase;

import java.io.IOException;
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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import v13.Day;
import v13.LimitOrder;
import v13.Order;
import v13.OrderBook;
import v13.PriceRecord;
import v13.agents.Agent;

import com.sun.istack.NotNull;

import fr.finaxys.tutorials.utils.AgentReferentialLine;
import fr.finaxys.tutorials.utils.HadoopTutorialException;
import fr.univlille1.atom.trace.TraceType;

abstract class AtomHBaseHelper {

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
	public static final String Q_AGENT_NAME = "AgentName";
	public static final char TICK = 'T';
	public static final char PRICE = 'P';
	public static final char ORDER = 'O';
	public static final char EXEC = 'E';
	public static final char DAY = 'D';
	public static final char AGENT = 'A';
	
	private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
			.getLogger(AtomHBaseHelper.class.getName());
	
	protected final HBaseDataTypeEncoder hbEncoder = new HBaseDataTypeEncoder();
	protected byte[] columnFamily;
	protected TableName tableName;
	//private Table table;
	protected Configuration hbaseConfiguration;
	private boolean open = false;
	private static AtomicLong idGen = new AtomicLong(1_000_000);
	
	public AtomHBaseHelper(byte[] columnFamily, TableName tableName) {
		this.columnFamily = columnFamily;
		this.tableName = tableName;
		this.hbaseConfiguration = createHbaseConfiguration();
	}
	
	public AtomHBaseHelper() {
		
	}
	
	public byte[] getColumnFamily() {
		return columnFamily;
	}

	public void setTableName(TableName tableName) {
		if (this.open) throw new HadoopTutorialException("TableName already set to "+this.tableName);
		this.tableName = tableName;
	}

	public void setColumnFamily(byte[] columnFamily) {
		if (this.open) throw new HadoopTutorialException("ColumnFamily already set to "+this.columnFamily);
		this.columnFamily = columnFamily;
	}
	
	
	public Configuration getHbaseConfiguration() {
		return hbaseConfiguration;
	}
	
	public void setHbaseConfiguration(Configuration hbaseConfiguration) {
		if (this.open) throw new HadoopTutorialException("hbaseConfiguration already set");
		this.hbaseConfiguration = hbaseConfiguration;
	}
	
	protected void closeTable() {
//		try {
			//table.close();
			//table = null;
			// @TODO should be manage in a thread safe manner?
			open = false;
//		} catch (IOException e) {
//			LOGGER.log(Level.SEVERE, "IO Exception while closing table", e);
//		}
	}
	
	protected void putTable(@NotNull Put p) {
		try {
			Table table = getTable();
			table.put(p);
		} catch (IOException e) {
			LOGGER.severe("Failed to push data into queue : " + e.getMessage());
			throw new HadoopTutorialException("Failed to push data into queue", e);
		}
	}
	
	protected  Scan mkScan() {
		Scan s = new Scan();
		s.addFamily(columnFamily);
		return s;
	}
	
	// @TODO can be handle more cleanly with callback to not expose Table 
	protected ResultScanner scanTable(@NotNull Scan p) {
		try {
			ResultScanner results = getTable().getScanner(p);
			return results;
		} catch (IOException e) {
			LOGGER.severe("Failed to push data into queue : " + e.getMessage());
			throw new HadoopTutorialException("Failed to push data into queue", e);
		}
	}
	
	protected Table getTable() throws IOException {
		Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);;
		return connection.getTable(tableName);
	}
	
	protected void returnTable(Table table) throws HadoopTutorialException {
		try {
			table.close();
		} catch (IOException e) {
			throw new HadoopTutorialException("cannot close table");
		}
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
		// @TODO should be manage in a thread safe manner?
		open = true;
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
	
	final protected void openTable() {
		assert !(tableName == null);
		if (open == true) return; // already initialized
//		String port = hbaseConfiguration.get("hbase.zookeeper.property.clientPort");
//		String host = hbaseConfiguration.get("hbase.zookeeper.quorum");
//
//		LOGGER.log(Level.INFO, "Try to connect to " + host + ":" + port);
//		LOGGER.log(Level.INFO, "Configuration completed");
		
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(hbaseConfiguration);
			createTable(connection);
			//this.table = createHTableConnexion(tableName, hbaseConfiguration);
			this.open = true;
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
	
	public  TraceType lookupType(@NotNull Result r) {
		byte[] row = r.getRow();
		byte c = row[row.length-1];
		LOGGER.log(Level.FINEST,"lookupType:" + c);
		TraceType type;
		switch (c) {
			case 'O' : type = TraceType.Order; break;
			case 'P' : type = TraceType.Price; break;
			case 'E' : type = TraceType.Exec; break;
			case 'A' : type = TraceType.Agent; break;
			case 'D' : type = TraceType.Day; break;
			case 'T' : type = TraceType.Tick; break;
			default: type = null; break;
		}
		return type; 
	}

	protected Put mkPutAgent(byte[] row, long ts, Agent a, Order o, PriceRecord pr) {
		Put p = new Put(row, ts);
		p.addColumn(columnFamily, Bytes.toBytes(Q_AGENT_NAME), ts,
				hbEncoder.encodeString(a.name));
		p.addColumn(columnFamily, Bytes.toBytes(Q_OB_NAME), ts,
				hbEncoder.encodeString(o.obName));
		p.addColumn(columnFamily, Bytes.toBytes(Q_CASH), hbEncoder.encodeLong(a.cash));
		p.addColumn(columnFamily, Bytes.toBytes(Q_EXECUTED_QUANTITY), ts,
				hbEncoder.encodeInt(pr.quantity));
		p.addColumn(columnFamily, Bytes.toBytes(Q_PRICE), ts,
				hbEncoder.encodeLong(pr.price));
		if (o.getClass().equals(LimitOrder.class)) {
			p.addColumn(columnFamily, Bytes.toBytes(Q_DIRECTION), ts,
					hbEncoder.encodeChar(((LimitOrder) o).direction));
			p.addColumn(columnFamily, Bytes.toBytes(Q_TIMESTAMP), ts,
					hbEncoder.encodeLong(pr.timestamp)); // pr.timestamp
			p.addColumn(columnFamily, Bytes.toBytes(Q_EXT_ORDER_ID), ts,
					hbEncoder.encodeString(o.extId));
		}
		return p;
	}

	@NotNull
	protected Put mkPutAgentReferential(@NotNull AgentReferentialLine agent, @NotNull HBaseDataTypeEncoder encoder,
			@NotNull byte[] columnF, long ts) {
			    Put p = new Put(Bytes.toBytes(agent.agentRefId + "R"), ts);
			    p.addColumn(columnF, Bytes.toBytes("agentRefId"), ts, encoder.encodeInt(agent.agentRefId));
			    p.addColumn(columnF, Bytes.toBytes("agentName"), ts, encoder.encodeString(agent.agentName));
			    p.addColumn(columnF, Bytes.toBytes("isMarketMaker"), ts, encoder.encodeBoolean(agent.isMarketMaker));
			    p.addColumn(columnF, Bytes.toBytes("details"), ts, encoder.encodeString(agent.details));
			    p.addColumn(columnFamily, Bytes.toBytes(Q_TIMESTAMP), ts, hbEncoder.encodeLong(ts));
			    return p;
			}

	protected Put mkPutOrderBook(byte[] row, long ts, int dayGap, int nbDays, OrderBook ob) {
		Put p = new Put(row, ts);
		p.addColumn(columnFamily, Bytes.toBytes(EXT_NUM_DAY), ts,
				hbEncoder.encodeInt(nbDays + dayGap));
		p.addColumn(columnFamily, Bytes.toBytes(Q_OB_NAME), ts,
				hbEncoder.encodeString(ob.obName));
		p.addColumn(columnFamily, Bytes.toBytes(Q_FIRST_FIXED_PRICE), ts,
				hbEncoder.encodeLong(ob.firstPriceOfDay));
		p.addColumn(columnFamily, Bytes.toBytes(Q_LOWEST_PRICE), ts,
				hbEncoder.encodeLong(ob.lowestPriceOfDay));
		p.addColumn(columnFamily, Bytes.toBytes(Q_HIGHEST_PRICE), ts,
				hbEncoder.encodeLong(ob.highestPriceOfDay));
		long price = 0;
		if (ob.lastFixedPrice != null) {
			price = ob.lastFixedPrice.price;
		}
		p.addColumn(columnFamily, Bytes.toBytes(Q_LAST_FIXED_PRICE), ts,
				hbEncoder.encodeLong(price));
		p.addColumn(columnFamily, Bytes.toBytes(Q_NB_PRICES_FIXED), ts,
				hbEncoder.encodeLong(ob.numberOfPricesFixed));
		p.addColumn(columnFamily, Bytes.toBytes(Q_TIMESTAMP), ts,
				hbEncoder.encodeLong(ts));
		return p;
	}

	protected Put mkPutExec(byte[] row, long ts, Order o) {
		Put p = new Put(row, ts);
		p.addColumn(columnFamily, Bytes.toBytes(Q_SENDER), ts,
				hbEncoder.encodeString(o.sender.name));
		p.addColumn(columnFamily, Bytes.toBytes(Q_EXT_ID), ts,
				hbEncoder.encodeString(o.extId));
		p.addColumn(columnFamily, Bytes.toBytes(Q_TIMESTAMP), ts,
				hbEncoder.encodeLong(ts));
		return p;
	}

	protected Put mkPutOrder(byte[] row, long ts, Order o) {
		Put p = new Put(row, ts);
		p.addColumn(columnFamily, Bytes.toBytes(Q_OB_NAME), ts,
				hbEncoder.encodeString(o.obName)); 
		p.addColumn(columnFamily, Bytes.toBytes(Q_SENDER), ts,
				hbEncoder.encodeString(o.sender.name));
		p.addColumn(columnFamily, Bytes.toBytes(Q_EXT_ID), ts, hbEncoder.encodeString(o.extId)); 
		p.addColumn(columnFamily, Bytes.toBytes(Q_TYPE), ts, hbEncoder.encodeChar(o.type));
		p.addColumn(columnFamily, Bytes.toBytes(Q_ID), ts, hbEncoder.encodeLong(o.id));
		p.addColumn(columnFamily, Bytes.toBytes(Q_TIMESTAMP), ts,
				hbEncoder.encodeLong(o.timestamp));
	
		if (o.getClass().equals(LimitOrder.class)) {
			LimitOrder lo = (LimitOrder) o;
			p.addColumn(columnFamily, Bytes.toBytes(Q_QUANTITY), ts,
					hbEncoder.encodeInt(lo.quantity));
			p.addColumn(columnFamily, Bytes.toBytes(Q_DIRECTION), ts,
					hbEncoder.encodeChar(lo.direction));
			p.addColumn(columnFamily, Bytes.toBytes(Q_PRICE), ts,
					hbEncoder.encodeLong(lo.price));
			p.addColumn(columnFamily, Bytes.toBytes(Q_VALIDITY), ts,
					hbEncoder.encodeLong(lo.validity));
		}
		return p;
	}

	protected Put mkPutPriceRecord(byte[] row, long ts, PriceRecord pr, long bestAskPrice,
			long bestBidPrice) {
				Put p = new Put(row, ts);
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
						hbEncoder.encodeLong((pr.timestamp > 0 ? pr.timestamp : ts)));
			
				return p;
			}

	protected Put mkPutTick(byte[] row, long ts, int dayGap, Day day, OrderBook ob) {
		Put p = new Put(row, ts);
		p.addColumn(columnFamily, Bytes.toBytes(Q_NUM_TICK), ts,
				hbEncoder.encodeInt(day.currentTick()));
		p.addColumn(columnFamily, Bytes.toBytes(Q_NUM_DAY), ts,
				hbEncoder.encodeInt(day.number + dayGap));
		p.addColumn(columnFamily, Bytes.toBytes(Q_OB_NAME), ts,
				hbEncoder.encodeString(ob.obName));
		p.addColumn(columnFamily, Bytes.toBytes(Q_TIMESTAMP), ts,
				hbEncoder.encodeLong(ts));
		if (!ob.ask.isEmpty()) {
			p.addColumn(columnFamily, Bytes.toBytes(Q_BEST_ASK), ts,
					hbEncoder.encodeLong(ob.ask.last().price));
		}
	
		if (!ob.bid.isEmpty()) {
			p.addColumn(columnFamily, Bytes.toBytes(Q_BEST_BID), ts,
					hbEncoder.encodeLong(ob.bid.last().price));
		}
	
		if (ob.lastFixedPrice != null) {
			p.addColumn(columnFamily, Bytes.toBytes(Q_LAST_FIXED_PRICE), ts,
					hbEncoder.encodeLong(ob.lastFixedPrice.price));
		}
		return p;
	}

	@NotNull
	protected byte[] createRequired(@NotNull char name) {
		long rowKey = Long.reverseBytes(idGen.incrementAndGet());
		return Bytes.toBytes(String.valueOf(rowKey) + name);
	}

	/**
	 * Absolutely not thread safe. Only used for Unit test
	 * @param name
	 * @return
	 */
	protected byte[] getLastRequired(@NotNull char name) {
		long rowKey = Long.reverseBytes(idGen.get());
		return Bytes.toBytes(String.valueOf(rowKey) + name);
	}

}
