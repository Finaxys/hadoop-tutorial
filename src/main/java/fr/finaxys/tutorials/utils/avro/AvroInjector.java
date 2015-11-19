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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import v13.*;
import v13.agents.Agent;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	private FileSystem fileSystem;
	private String destHDFS;
    private GenericRecord orderRecord;
    private GenericRecord priceRecord;
    private GenericRecord execRecord;
    private GenericRecord tickRecord;
    private GenericRecord dayRecord;
    private GenericRecord agentRecord;
    private GenericRecord agentRefRecord;
	private TimeStampBuilder tsb;
    private int dayGap;
    private String pathSchema ;
    private Map<String,DataFileWriter<GenericRecord>> fileWriters ;

	public AvroInjector(@NotNull AtomConfiguration atomConf) throws Exception {
		this.atomConf = atomConf;
		this.destHDFS = atomConf.getDestHDFS();
		
		// boolean isAvro = atomConf.isOutAvro();
		this.conf = new Configuration();
		this.conf.addResource(new Path(atomConf.getHadoopConfCore()));
		this.conf.addResource(new Path(atomConf.getHadoopConfHdfs()));
		this.conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		this.conf.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());
        this.dayGap = atomConf.getDayGap();
        this.pathSchema = atomConf.getAvroSchema();
        this.fileWriters = new HashMap<String,DataFileWriter<GenericRecord>>() ;
	}

	public GenericRecord createRecord(String schemaPath,String hdfsDest,String key) throws IOException {
        Schema schema = new Schema.Parser().parse(new File(schemaPath));
        DatumWriter<GenericRecord> orderDatumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(orderDatumWriter);
        FSDataOutputStream file = fileSystem.create(new Path(hdfsDest));
        dataFileWriter.create(schema, file);
        fileWriters.put(key, dataFileWriter);
        return new GenericData.Record(schema);
	}

	// one schema for each agent ?

	@Override
	public void createOutput() {
		try {
			LOGGER.info("Create output ...");
            fileSystem = FileSystem.get(conf);

            orderRecord = createRecord(pathSchema+"/order.avsc",destHDFS+"orderFile","order");
            priceRecord = createRecord(pathSchema+"/price.avsc",destHDFS+"priceFile","price");
            tickRecord = createRecord(pathSchema+"/tick.avsc",destHDFS+"tickFile","tick");
            dayRecord = createRecord(pathSchema+"/day.avsc",destHDFS+"dayFile","day");
            agentRecord = createRecord(pathSchema+"/agent.avsc",destHDFS+"agentFile","agent");
            execRecord = createRecord(pathSchema+"/exec.avsc",destHDFS+"execFile","exec");
            agentRefRecord = createRecord(pathSchema+"/agentRef.avsc",destHDFS+"agentRefFile","agentRef");

		} catch (IOException e) {
			throw new HadoopTutorialException("Cannot create Avro output", e);
		}
	}

	@Override
	public void sendAgent(Agent a, Order o, PriceRecord pr) {
		try {
            //put agent fields 
            agentRecord.put(Q_TRACE_TYPE,TraceType.Agent.name());
            agentRecord.put(Q_AGENT_NAME,a.name);
            agentRecord.put( Q_OB_NAME, o.obName);
            agentRecord.put( Q_CASH, a.cash);
            agentRecord.put(Q_EXECUTED_QUANTITY,pr.quantity);
            agentRecord.put(Q_PRICE,pr.price);
            if (o.getClass().equals(LimitOrder.class)) {
                agentRecord.put(Q_DIRECTION,((LimitOrder) o).direction+"");
                agentRecord.put(Q_TIMESTAMP, pr.timestamp); // pr.timestamp
                agentRecord.put(Q_EXT_ORDER_ID,o.extId);
            }
            //append agent
			fileWriters.get("agent").append(agentRecord);
		} catch (IOException e) {
			throw new HadoopTutorialException("Cannot create Avro output", e);
		}
	}

	@Override
	public void sendPriceRecord(PriceRecord pr, long bestAskPrice,
			long bestBidPrice) {
		try {
            long ts = tsb.nextTimeStamp();
            //put price fields 
            priceRecord.put(Q_TRACE_TYPE, TraceType.Price.name());
            priceRecord.put(Q_OB_NAME, pr.obName);
            priceRecord.put(Q_PRICE, pr.price);
            priceRecord.put(Q_EXECUTED_QUANTITY, pr.quantity);
            priceRecord.put( Q_DIR,pr.dir+"");
            priceRecord.put( Q_ORDER1,pr.extId1);
            priceRecord.put( Q_ORDER2, pr.extId2);
            priceRecord.put(Q_BEST_ASK, bestAskPrice);
            priceRecord.put( Q_BEST_BID,bestBidPrice);
            priceRecord.put( Q_TIMESTAMP,(pr.timestamp > 0 ? pr.timestamp : ts));
            //append price 
            fileWriters.get("price").append(priceRecord);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void sendAgentReferential(List<AgentReferentialLine> referencial) {

        for (AgentReferentialLine agent : referencial) {
            long ts = tsb.nextTimeStamp();
            //put agent fields
            agentRefRecord.put("agentRefId",agent.agentRefId);
            agentRefRecord.put("agentName",agent.agentName);
            agentRefRecord.put("isMarketMaker", agent.isMarketMaker);
            agentRefRecord.put("details",agent.details);
            agentRefRecord.put(Q_TIMESTAMP, ts);
            //append agent
            try {
                fileWriters.get("agentRef").append(agentRefRecord);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
	}

	@Override
	public void sendOrder(Order o) {
		try {
            //put data fields 
            orderRecord.put( Q_TRACE_TYPE,TraceType.Order.name());
            orderRecord.put( Q_OB_NAME,o.obName);
            orderRecord.put( Q_SENDER,o.sender.name);
            orderRecord.put( Q_EXT_ID, o.extId);
            orderRecord.put( Q_TYPE, o.type+"");
            orderRecord.put( Q_ID,o.id);
            orderRecord.put( Q_TIMESTAMP,o.timestamp);

            if (o.getClass().equals(LimitOrder.class)) {
                LimitOrder lo = (LimitOrder) o;
                orderRecord.put( Q_QUANTITY,lo.quantity);
                orderRecord.put( Q_DIRECTION,lo.direction+"");
                orderRecord.put( Q_PRICE,lo.price);
                orderRecord.put( Q_VALIDITY,lo.validity);
            }
            //append data
            fileWriters.get("order").append(orderRecord);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void sendTick(Day day, Collection<OrderBook> orderbooks) {
		try {
            long ts = tsb.nextTimeStamp();
            for (OrderBook ob : orderbooks) {
                //put tick fields
                tickRecord.put( Q_TRACE_TYPE, TraceType.Tick.name());
                tickRecord.put( Q_NUM_TICK,day.currentTick());
                tickRecord.put( Q_NUM_DAY,day.number + dayGap);
                tickRecord.put( Q_OB_NAME,ob.obName);
                tickRecord.put( Q_TIMESTAMP,ts);
                if (!ob.ask.isEmpty()) {
                    tickRecord.put(Q_BEST_ASK,ob.ask.last().price);
                }
                if (!ob.bid.isEmpty()) {
                    tickRecord.put(Q_BEST_BID,ob.bid.last().price);
                }
                if (ob.lastFixedPrice != null) {
                    tickRecord.put(Q_LAST_FIXED_PRICE,ob.lastFixedPrice.price);
                }
                //append tick
                fileWriters.get("tick").append(tickRecord);
            }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void sendDay(int nbDays, Collection<OrderBook> orderbooks) {
		try {
            long ts = tsb.nextTimeStamp();
            for (OrderBook ob : orderbooks) {
                //put day fields
                dayRecord.put(Q_TRACE_TYPE, TraceType.Day.name());
                dayRecord.put(EXT_NUM_DAY, nbDays + dayGap);
                dayRecord.put( Q_OB_NAME, ob.obName);
                dayRecord.put(Q_FIRST_FIXED_PRICE, ob.firstPriceOfDay);
                dayRecord.put(Q_LOWEST_PRICE, ob.lowestPriceOfDay);
                dayRecord.put( Q_HIGHEST_PRICE, ob.highestPriceOfDay);
                long price = 0;
                if (ob.lastFixedPrice != null) {
                    price = ob.lastFixedPrice.price;
                }
                dayRecord.put( Q_LAST_FIXED_PRICE, price);
                dayRecord.put( Q_NB_PRICES_FIXED, ob.numberOfPricesFixed);
                dayRecord.put( Q_TIMESTAMP, ts);
                //append day
                fileWriters.get("day").append(dayRecord);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void sendExec(Order o) {
		try {
            long ts = tsb.nextTimeStamp();
            //put exec fields
            execRecord.put(Q_TRACE_TYPE, TraceType.Exec.name());
            execRecord.put(Q_SENDER,  o.sender.name);
            execRecord.put( Q_EXT_ID,o.extId);
            execRecord.put( Q_TIMESTAMP,ts);
            //append exec
            fileWriters.get("exec").append(execRecord);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void closeOutput() {
		try {
            for(Map.Entry<String, DataFileWriter<GenericRecord>> entry  : this.fileWriters.entrySet()){
                entry.getValue().close();
            }
/*            // to verify
            BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem.open(new Path("/priceFile"))));
            String line;
            line=br.readLine();
            while (line != null){
                System.out.println(line);
                line=br.readLine();
            }*/

                fileSystem.close();
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
