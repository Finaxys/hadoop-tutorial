package fr.finaxys.tutorials.utils.hbase;

import com.sun.istack.NotNull;
import fr.finaxys.tutorials.utils.AgentReferentialLine;
import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.HadoopTutorialException;
import fr.univlille1.atom.trace.TraceType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import v13.*;
import v13.agents.Agent;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

abstract class AtomHBaseHelper {

	public static final byte[] Q_TRACE_TYPE = Bytes.toBytes("Trace");
	public static final byte[] Q_NUM_DAY = Bytes.toBytes("NumDay");
	public static final byte[] Q_NUM_TICK = Bytes.toBytes("NumTick");
	public static final byte[] Q_BEST_BID = Bytes.toBytes("BestBid");
	public static final byte[] Q_BEST_ASK = Bytes.toBytes("BestAsk");
	public static final byte[] Q_ORDER2 = Bytes.toBytes("Order2");
	public static final byte[] Q_ORDER1 = Bytes.toBytes("Order1");
	public static final byte[] Q_DIR = Bytes.toBytes("Dir");
	public static final byte[] Q_OB_NAME = Bytes.toBytes("ObName");
	public static final byte[] Q_VALIDITY = Bytes.toBytes("Validity");
	public static final byte[] Q_QUANTITY = Bytes.toBytes("Quantity");
	public static final byte[] Q_ID = Bytes.toBytes("Id");
	public static final byte[] Q_TYPE = Bytes.toBytes("Type");
	public static final byte[] Q_EXT_ID = Bytes.toBytes("ExtId");
	public static final byte[] Q_SENDER = Bytes.toBytes("Sender");
	public static final byte[] Q_NB_PRICES_FIXED = Bytes.toBytes("NbPricesFixed");
	public static final byte[] Q_LAST_FIXED_PRICE = Bytes.toBytes("LastFixedPrice");
	public static final byte[] Q_HIGHEST_PRICE = Bytes.toBytes("HighestPrice");
	public static final byte[] Q_LOWEST_PRICE = Bytes.toBytes("LowestPrice");
	public static final byte[] Q_FIRST_FIXED_PRICE = Bytes.toBytes("FirstFixedPrice");
	public static final byte[] EXT_NUM_DAY = Bytes.toBytes("NumDay");
	public static final byte[] Q_EXT_ORDER_ID = Bytes.toBytes("OrderExtId");
	public static final byte[] Q_TIMESTAMP = Bytes.toBytes("Timestamp");
	public static final byte[] Q_DIRECTION = Bytes.toBytes("Direction");
	public static final byte[] Q_PRICE = Bytes.toBytes("Price");
	public static final byte[] Q_EXECUTED_QUANTITY = Bytes.toBytes("Executed");
	public static final byte[] Q_CASH = Bytes.toBytes("Cash");
	public static final byte[] Q_AGENT_NAME = Bytes.toBytes("AgentName");
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
	private Table table;
	private BufferedMutator mutator;
	protected Configuration configuration;
	private boolean open = false;
	private static AtomicLong idGen = new AtomicLong(1_000_000);
	private Connection connection;
	private boolean useBuffer = true;
	
	public AtomHBaseHelper(byte[] columnFamily, TableName tableName, Configuration configuration) {
		this.columnFamily = columnFamily;
		this.tableName = tableName;
		this.configuration = configuration;
	}
	
	public AtomHBaseHelper(byte[] columnFamily, TableName tableName,AtomConfiguration atomConf) {
		this.columnFamily = columnFamily;
		this.tableName = tableName;
		this.configuration = createHbaseConfiguration(atomConf);
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
		return configuration;
	}
	
	public void setHbaseConfiguration(Configuration hbaseConfiguration) {
		if (this.open) throw new HadoopTutorialException("hbaseConfiguration already set");
		this.configuration = hbaseConfiguration;
	}
	
	protected void closeTable() {
//		try {
			//table.close();
			//table = null;
			// @TODO should be manage in a thread safe manner?
		try {
			if (useBuffer) {
				mutator.close();
				table.close();
				mutator = null;
				table = null;
			}
			connection.close();
			connection = null;
			open = false;
		} catch (IOException e) {
			LOGGER.severe("Failed to cloce connection " + e.getMessage() );
			throw new HadoopTutorialException("Failed to push data into queue", e);
		}
			
//		} catch (IOException e) {
//			LOGGER.log(Level.SEVERE, "IO Exception while closing table", e);
//		}
	}
	
	protected void flushIfNeeded() {
		try {
			if (useBuffer) {
				mutator.flush();
			}
		} catch (IOException e) {
			LOGGER.severe("Failed to flush mutator " + e.getMessage() );
			throw new HadoopTutorialException("Failed to flush mutator", e);
		}
	}
	
	protected void putTable(@NotNull Put p) {
		try {
			Table table = getTable();
			if (useBuffer) {
				mutator.mutate(p);
			} else {
				table.put(p);
			}
			returnTable(table);
		} catch (IOException e) {
			LOGGER.severe("Failed to push data into queue : " + e.getMessage());
			throw new HadoopTutorialException("Failed to push data into queue", e);
		}
	}
	
	protected void directPutTable(@NotNull Put p) {
		try {
			Table table = getTable();
			table.put(p);
			returnTable(table);
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
	protected ResultScanner scanTable(@NotNull Table table, @NotNull Scan s) {
		try {
			ResultScanner results = table.getScanner(s);
			return results;
		} catch (IOException e) {
			LOGGER.severe("Failed to push data into queue : " + e.getMessage());
			throw new HadoopTutorialException("Failed to push data into queue", e);
		}
	}
	
	protected Table getTable() throws HadoopTutorialException {
		try {
			LOGGER.log(Level.FINEST,"Getting table "+ tableName.getQualifierAsString());
			if (useBuffer) {
				if (!open) {
					openTable();
				}
				return table;
			} else {
				return connection.getTable(tableName);
			}
		} catch (IOException e) {
			throw new HadoopTutorialException("cannot get table");
		}	
	}
	
	protected void returnTable(Table table) throws HadoopTutorialException {
		try {
			LOGGER.log(Level.FINEST,"Returning table "+ table.getName().getQualifierAsString());
			if (!useBuffer) {
				table.close();
			}
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
	
	private Configuration createHbaseConfiguration(AtomConfiguration atomConf) {
		Configuration conf = HBaseConfiguration.create();
		try {
		conf.addResource(new File(atomConf.getHadoopConfCore()).getAbsoluteFile().toURI().toURL());
		conf.addResource(new
		File(atomConf.getHbaseConfHbase()).getAbsoluteFile().toURI().toURL());
            conf.addResource(new
		File(atomConf.getHadoopConfHdfs()).getAbsoluteFile().toURI().toURL());
		} catch (MalformedURLException e) {
		LOGGER.log(Level.SEVERE, "Could not get hbase configuration files",
		e);
		throw new HadoopTutorialException("hbase", e);
		}
		conf.reloadConfiguration();
		return conf;
	}

//	private Table createHTableConnexion(TableName tableName,
//			Configuration hbConf) throws IOException {
//		Connection connection = ConnectionFactory.createConnection(hbConf);
//		Table table = connection.getTable(tableName);
//		return table;
//	}
	
	final protected void openTable() {
		assert !(tableName == null);
		if (open == true) return; // already initialized
//		String port = hbaseConfiguration.get("hbase.zookeeper.property.clientPort");
//		String host = hbaseConfiguration.get("hbase.zookeeper.quorum");
//
//		LOGGER.log(Level.INFO, "Try to connect to " + host + ":" + port);
//		LOGGER.log(Level.INFO, "Configuration completed");
		try {
			connection = ConnectionFactory.createConnection(configuration);
			createTable(connection);
			//this.table = createHTableConnexion(tableName, hbaseConfiguration);
			if (useBuffer) {
				this.table = connection.getTable(tableName);
			    this.mutator = connection.getBufferedMutator(tableName);
			}
			this.open = true;
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Could not create Connection", e);
			throw new HadoopTutorialException("hbase connection", e);
		}/* finally {
			if (connection != null)
				try {
					connection.close();
				} catch (IOException e) {
					// can't do anything here...
				}
		}*/
	}
	
	public static TraceType lookupType(@NotNull byte[] row) {
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
		p.addColumn(columnFamily, Q_TRACE_TYPE, ts,
				hbEncoder.encodeString(TraceType.Agent.name()));
		p.addColumn(columnFamily, Q_AGENT_NAME, ts,
				hbEncoder.encodeString(a.name));
		p.addColumn(columnFamily, Q_OB_NAME, ts,
				hbEncoder.encodeString(o.obName));
		p.addColumn(columnFamily, Q_CASH, hbEncoder.encodeLong(a.cash));
		p.addColumn(columnFamily, Q_EXECUTED_QUANTITY, ts,
				hbEncoder.encodeInt(pr.quantity));
		p.addColumn(columnFamily, Q_PRICE, ts,
				hbEncoder.encodeLong(pr.price));
		if (o.getClass().equals(LimitOrder.class)) {
			p.addColumn(columnFamily, Q_DIRECTION, ts,
					hbEncoder.encodeChar(((LimitOrder) o).direction));
			p.addColumn(columnFamily, Q_TIMESTAMP, ts,
					hbEncoder.encodeLong(pr.timestamp)); // pr.timestamp
			p.addColumn(columnFamily, Q_EXT_ORDER_ID, ts,
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
	    p.addColumn(columnFamily, Q_TIMESTAMP, ts, hbEncoder.encodeLong(ts));
	    return p;
	}

	protected Put mkPutDay(byte[] row, long ts, int dayGap, int nbDays, OrderBook ob) {
		Put p = new Put(row, ts);
		p.addColumn(columnFamily, Q_TRACE_TYPE, ts,
				hbEncoder.encodeString(TraceType.Day.name()));
		p.addColumn(columnFamily, EXT_NUM_DAY, ts,
				hbEncoder.encodeInt(nbDays + dayGap));
		p.addColumn(columnFamily, Q_OB_NAME, ts,
				hbEncoder.encodeString(ob.obName));
		p.addColumn(columnFamily, Q_FIRST_FIXED_PRICE, ts,
				hbEncoder.encodeLong(ob.firstPriceOfDay));
		p.addColumn(columnFamily, Q_LOWEST_PRICE, ts,
				hbEncoder.encodeLong(ob.lowestPriceOfDay));
		p.addColumn(columnFamily, Q_HIGHEST_PRICE, ts,
				hbEncoder.encodeLong(ob.highestPriceOfDay));
		long price = 0;
		if (ob.lastFixedPrice != null) {
			price = ob.lastFixedPrice.price;
		}
		p.addColumn(columnFamily, Q_LAST_FIXED_PRICE, ts,
				hbEncoder.encodeLong(price));
		p.addColumn(columnFamily, Q_NB_PRICES_FIXED, ts,
				hbEncoder.encodeLong(ob.numberOfPricesFixed));
		p.addColumn(columnFamily, Q_TIMESTAMP, ts,
				hbEncoder.encodeLong(ts));
		return p;
	}

	protected Put mkPutExec(byte[] row, long ts, Order o) {
		Put p = new Put(row, ts);
		p.addColumn(columnFamily, Q_TRACE_TYPE, ts,
				hbEncoder.encodeString(TraceType.Exec.name()));
		p.addColumn(columnFamily, Q_SENDER, ts,
				hbEncoder.encodeString(o.sender.name));
		p.addColumn(columnFamily, Q_EXT_ID, ts,
				hbEncoder.encodeString(o.extId));
		p.addColumn(columnFamily, Q_TIMESTAMP, ts,
				hbEncoder.encodeLong(ts));
		return p;
	}

	protected Put mkPutOrder(byte[] row, long ts, Order o) {
		Put p = new Put(row, ts);
		p.addColumn(columnFamily, Q_TRACE_TYPE, ts,
				hbEncoder.encodeString(TraceType.Order.name()));
		p.addColumn(columnFamily, Q_OB_NAME, ts,
				hbEncoder.encodeString(o.obName)); 
		p.addColumn(columnFamily, Q_SENDER, ts,
				hbEncoder.encodeString(o.sender.name));
		p.addColumn(columnFamily, Q_EXT_ID, ts, hbEncoder.encodeString(o.extId)); 
		p.addColumn(columnFamily, Q_TYPE, ts, hbEncoder.encodeChar(o.type));
		p.addColumn(columnFamily, Q_ID, ts, hbEncoder.encodeLong(o.id));
		p.addColumn(columnFamily, Q_TIMESTAMP, ts,
				hbEncoder.encodeLong(o.timestamp));
	
		if (o.getClass().equals(LimitOrder.class)) {
			LimitOrder lo = (LimitOrder) o;
			p.addColumn(columnFamily, Q_QUANTITY, ts,
					hbEncoder.encodeInt(lo.quantity));
			p.addColumn(columnFamily, Q_DIRECTION, ts,
					hbEncoder.encodeChar(lo.direction));
			p.addColumn(columnFamily, Q_PRICE, ts,
					hbEncoder.encodeLong(lo.price));
			p.addColumn(columnFamily, Q_VALIDITY, ts,
					hbEncoder.encodeLong(lo.validity));
		}
		return p;
	}

	protected Put mkPutPriceRecord(byte[] row, long ts, PriceRecord pr, long bestAskPrice,
			long bestBidPrice) {
				Put p = new Put(row, ts);
				p.addColumn(columnFamily, Q_TRACE_TYPE, ts,
						hbEncoder.encodeString(TraceType.Price.name()));
				p.addColumn(columnFamily, Q_OB_NAME, ts,
						hbEncoder.encodeString(pr.obName));
				p.addColumn(columnFamily, Q_PRICE, ts,
						hbEncoder.encodeLong(pr.price));
				p.addColumn(columnFamily, Q_EXECUTED_QUANTITY, ts,
						hbEncoder.encodeInt(pr.quantity));
				p.addColumn(columnFamily, Q_DIR, ts,
						hbEncoder.encodeChar(pr.dir));
				p.addColumn(columnFamily, Q_ORDER1, ts,
						hbEncoder.encodeString(pr.extId1));
				p.addColumn(columnFamily, Q_ORDER2, ts,
						hbEncoder.encodeString(pr.extId2));
				p.addColumn(columnFamily, Q_BEST_ASK, ts,
						hbEncoder.encodeLong(bestAskPrice));
				p.addColumn(columnFamily, Q_BEST_BID, ts,
						hbEncoder.encodeLong(bestBidPrice));
				p.addColumn(columnFamily, Q_TIMESTAMP, ts,
						hbEncoder.encodeLong((pr.timestamp > 0 ? pr.timestamp : ts)));
			
				return p;
			}

	protected Put mkPutTick(byte[] row, long ts, int dayGap, Day day, OrderBook ob) {
		Put p = new Put(row, ts);
		p.addColumn(columnFamily, Q_TRACE_TYPE, ts,
				hbEncoder.encodeString(TraceType.Tick.name()));
		p.addColumn(columnFamily, Q_NUM_TICK, ts,
				hbEncoder.encodeInt(day.currentTick()));
		p.addColumn(columnFamily, Q_NUM_DAY, ts,
				hbEncoder.encodeInt(day.number + dayGap));
		p.addColumn(columnFamily, Q_OB_NAME, ts,
				hbEncoder.encodeString(ob.obName));
		p.addColumn(columnFamily, Q_TIMESTAMP, ts,
				hbEncoder.encodeLong(ts));
		if (!ob.ask.isEmpty()) {
			p.addColumn(columnFamily, Q_BEST_ASK, ts,
					hbEncoder.encodeLong(ob.ask.last().price));
		}
	
		if (!ob.bid.isEmpty()) {
			p.addColumn(columnFamily, Q_BEST_BID, ts,
					hbEncoder.encodeLong(ob.bid.last().price));
		}
	
		if (ob.lastFixedPrice != null) {
			p.addColumn(columnFamily, Q_LAST_FIXED_PRICE, ts,
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

	public boolean isUseBuffer() {
		return useBuffer;
	}

	public void setUseBuffer(boolean useBuffer) {
		if (this.open) throw new HadoopTutorialException("TableName already open "+this.tableName);
		this.useBuffer = useBuffer;
	}

}
