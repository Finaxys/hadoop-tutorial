package fr.finaxys.tutorials.utils.avro;

import com.sun.istack.NotNull;
import fr.finaxys.tutorials.utils.*;
import fr.univlille1.atom.trace.TraceType;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import v13.*;
import v13.agents.Agent;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class AvroInjector implements AtomDataInjector {
	private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
			.getLogger(AtomDataInjector.class.getName());

    public static final String Q_TRACE_TYPE = "Trace";
    public static final String Q_NUM_DAY ="NumDay";
    public static final String Q_NUM_TICK ="NumTick";
    public static final String Q_BEST_BID ="BestBid";
    public static final String Q_BEST_ASK ="BestAsk";
    public static final String Q_ORDER2 ="Order2";
    public static final String Q_ORDER1 ="Order1";
    public static final String Q_DIR ="Dir";
    public static final String Q_OB_NAME ="ObName";
    public static final String Q_VALIDITY ="Validity";
    public static final String Q_QUANTITY ="Quantity";
    public static final String Q_ID ="Id";
    public static final String Q_TYPE ="Type";
    public static final String Q_EXT_ID ="ExtId";
    public static final String Q_SENDER ="Sender";
    public static final String Q_NB_PRICES_FIXED ="NbPricesFixed";
    public static final String Q_LAST_FIXED_PRICE ="LastFixedPrice";
    public static final String Q_HIGHEST_PRICE ="HighestPrice";
    public static final String Q_LOWEST_PRICE ="LowestPrice";
    public static final String Q_FIRST_FIXED_PRICE ="FirstFixedPrice";
    public static final String EXT_NUM_DAY ="NumDay";
    public static final String Q_EXT_ORDER_ID ="OrderExtId";
    public static final String Q_TIMESTAMP ="Timestamp";
    public static final String Q_DIRECTION ="Direction";
    public static final String Q_PRICE ="Price";
    public static final String Q_EXECUTED_QUANTITY ="Executed";
    public static final String Q_CASH ="Cash";
    public static final String Q_AGENT_NAME ="AgentName";

	private final AtomConfiguration atomConf;
	private Configuration conf;
	private Schema schema;
    private Schema orderSchema;
    private Schema priceSchema;
    private Schema execSchema;
    private Schema tickSchema;
    private Schema daySchema;
    private Schema agentSchema;
    private Schema agentRefSchema;
	private FileSystem fileSystem;
	private String destHDFS;
	private Path pathDestHDFS;
	private String pathAvroFile;
    private File file ;
	private DataFileWriter<GenericRecord> dataFileWriter;
    private DataFileWriter<GenericRecord> orderFileWriter;
    private DataFileWriter<GenericRecord> priceFileWriter;
    private DataFileWriter<GenericRecord> execFileWriter;
    private DataFileWriter<GenericRecord> tickFileWriter;
    private DataFileWriter<GenericRecord> dayFileWriter;
    private DataFileWriter<GenericRecord> agentFileWriter;
    private DataFileWriter<GenericRecord> agentRefFileWriter;
	private GenericRecord genericRecord;
    private GenericRecord orderRecord;
    private GenericRecord priceRecord;
    private GenericRecord execRecord;
    private GenericRecord tickRecord;
    private GenericRecord dayRecord;
    private GenericRecord agentRecord;
    private GenericRecord agentRefRecord;
	private TimeStampBuilder tsb;
    private int dayGap;

	public AvroInjector(@NotNull AtomConfiguration atomConf) throws Exception {
		this.atomConf = atomConf;
		this.destHDFS = atomConf.getDestHDFS();
		this.pathAvroFile = atomConf.getPathAvro();
		String pathSchema = atomConf.getAvroSchema();
		// boolean isAvro = atomConf.isOutAvro();
		this.conf = new Configuration();
		this.conf.addResource(new Path(atomConf.getHadoopConfCore()));
		this.conf.addResource(new Path(atomConf.getHadoopConfHdfs()));
		this.conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		this.conf.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());
		this.schema = new Schema.Parser().parse(new File(pathSchema));
        this.dayGap = atomConf.getDayGap();
	}

	public void sendToHDFS(Path path) throws IOException {
		try {
			fileSystem = FileSystem.get(conf);
			pathDestHDFS = new Path(destHDFS);
			fileSystem.copyFromLocalFile(false, true, path, pathDestHDFS);
            System.out.println(" ");
		} catch (Exception e) {
			LOGGER.severe("Exception : " + e);
		} finally {
			fileSystem.close();
		}
	}

	// one schema for each agent ?

	@Override
	public void createOutput() {
		try {
			LOGGER.info("Create output ...");
			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(
					schema);
			this.dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
			this.file = new File(pathAvroFile);
			this.dataFileWriter.create(schema, file);
			this.genericRecord = new GenericData.Record(schema);
		} catch (IOException e) {
			throw new HadoopTutorialException("Cannot create Avro output", e);
		}
	}

	@Override
	public void sendAgent(Agent a, Order o, PriceRecord pr) {
/*		try {
            //put data fields 
            genericRecord.put(Q_TRACE_TYPE,TraceType.Agent.name());
            genericRecord.put(Q_AGENT_NAME,a.name);
            genericRecord.put( Q_OB_NAME, o.obName);
            genericRecord.put( Q_CASH, a.cash);
            genericRecord.put(Q_EXECUTED_QUANTITY,pr.quantity);
            genericRecord.put(Q_PRICE,pr.price);
            if (o.getClass().equals(LimitOrder.class)) {
                genericRecord.put(Q_DIRECTION,((LimitOrder) o).direction);
                genericRecord.put(Q_TIMESTAMP, pr.timestamp); // pr.timestamp
                genericRecord.put(Q_EXT_ORDER_ID,o.extId);
            }
            //append data
			dataFileWriter.append(genericRecord);
		} catch (IOException e) {
			throw new HadoopTutorialException("Cannot create Avro output", e);
		}*/
	}

	@Override
	public void sendPriceRecord(PriceRecord pr, long bestAskPrice,
			long bestBidPrice) {
/*		try {
            long ts = tsb.nextTimeStamp();
            //put data fields 
            genericRecord.put(Q_TRACE_TYPE, TraceType.Price.name());
            genericRecord.put(Q_OB_NAME, pr.obName);
            genericRecord.put(Q_PRICE, pr.price);
            genericRecord.put(Q_EXECUTED_QUANTITY, pr.quantity);
            genericRecord.put( Q_DIR,pr.dir);
            genericRecord.put( Q_ORDER1,pr.extId1);
            genericRecord.put( Q_ORDER2, pr.extId2);
            genericRecord.put(Q_BEST_ASK, bestAskPrice);
            genericRecord.put( Q_BEST_BID,bestBidPrice);
            genericRecord.put( Q_TIMESTAMP,(pr.timestamp > 0 ? pr.timestamp : ts));
            //append data 
			dataFileWriter.append(genericRecord);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}

	@Override
	public void sendAgentReferential(List<AgentReferentialLine> referencial) {

/*        for (AgentReferentialLine agent : referencial) {
            long ts = tsb.nextTimeStamp();
            //put data fields
            genericRecord.put("agentRefId",agent.agentRefId);
            genericRecord.put("agentName",agent.agentName);
            genericRecord.put("isMarketMaker", agent.isMarketMaker);
            genericRecord.put("details",agent.details);
            genericRecord.put(Q_TIMESTAMP, ts);
            //append data
            try {
                dataFileWriter.append(genericRecord);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }*/
	}

	@Override
	public void sendOrder(Order o) {
		try {
            //put data fields 
            genericRecord.put( Q_TRACE_TYPE,TraceType.Order.name());
            genericRecord.put( Q_OB_NAME,o.obName);
            genericRecord.put( Q_SENDER,o.sender.name);
            genericRecord.put( Q_EXT_ID, o.extId);
            genericRecord.put( Q_TYPE, o.type+"");
            genericRecord.put( Q_ID,o.id);
            genericRecord.put( Q_TIMESTAMP,o.timestamp);

            if (o.getClass().equals(LimitOrder.class)) {
                LimitOrder lo = (LimitOrder) o;
                genericRecord.put( Q_QUANTITY,lo.quantity);
                genericRecord.put( Q_DIRECTION,lo.direction+"");
                genericRecord.put( Q_PRICE,lo.price);
                genericRecord.put( Q_VALIDITY,lo.validity);
            }
            //append data
			dataFileWriter.append(genericRecord);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void sendTick(Day day, Collection<OrderBook> orderbooks) {
/*		try {
            long ts = tsb.nextTimeStamp();
            for (OrderBook ob : orderbooks) {
                //put data fields
                genericRecord.put( Q_TRACE_TYPE, TraceType.Tick.name());
                genericRecord.put( Q_NUM_TICK,day.currentTick());
                genericRecord.put( Q_NUM_DAY,day.number + dayGap);
                genericRecord.put( Q_OB_NAME,ob.obName);
                genericRecord.put( Q_TIMESTAMP,ts);
                if (!ob.ask.isEmpty()) {
                    genericRecord.put(Q_BEST_ASK,ob.ask.last().price);
                }
                if (!ob.bid.isEmpty()) {
                    genericRecord.put(Q_BEST_BID,ob.bid.last().price);
                }
                if (ob.lastFixedPrice != null) {
                    genericRecord.put(Q_LAST_FIXED_PRICE,ob.lastFixedPrice.price);
                }
                //append data
                dataFileWriter.append(genericRecord);
            }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}

	@Override
	public void sendDay(int nbDays, Collection<OrderBook> orderbooks) {
/*		try {
            long ts = tsb.nextTimeStamp();
            for (OrderBook ob : orderbooks) {
                //put data fields
                genericRecord.put(Q_TRACE_TYPE, TraceType.Day.name());
                genericRecord.put(EXT_NUM_DAY, nbDays + dayGap);
                genericRecord.put( Q_OB_NAME, ob.obName);
                genericRecord.put(Q_FIRST_FIXED_PRICE, ob.firstPriceOfDay);
                genericRecord.put(Q_LOWEST_PRICE, ob.lowestPriceOfDay);
                genericRecord.put( Q_HIGHEST_PRICE, ob.highestPriceOfDay);
                long price = 0;
                if (ob.lastFixedPrice != null) {
                    price = ob.lastFixedPrice.price;
                }
                genericRecord.put( Q_LAST_FIXED_PRICE, price);
                genericRecord.put( Q_NB_PRICES_FIXED, ob.numberOfPricesFixed);
                genericRecord.put( Q_TIMESTAMP, ts);
                //append data
				dataFileWriter.append(genericRecord);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}

	@Override
	public void sendExec(Order o) {
/*		try {
            long ts = tsb.nextTimeStamp();
            //put data type
			genericRecord.put("type", "Exec");
            //put data fields
            genericRecord.put(Q_TRACE_TYPE, TraceType.Exec.name());
            genericRecord.put(Q_SENDER,  o.sender.name);
            genericRecord.put( Q_EXT_ID,o.extId);
            genericRecord.put( Q_TIMESTAMP,ts);
            //append data
			dataFileWriter.append(genericRecord);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}

	@Override
	public void closeOutput() {
		try {
            dataFileWriter.close();
			Path pathAvro = new Path(pathAvroFile);
			sendToHDFS(pathAvro);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void setTimeStampBuilder(TimeStampBuilder tsb) {
		this.tsb = tsb;
	}

	@Override
	public TimeStampBuilder getTimeStampBuilder() {
		return tsb;
	}
}
