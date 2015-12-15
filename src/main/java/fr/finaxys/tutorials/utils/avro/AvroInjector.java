package fr.finaxys.tutorials.utils.avro;

import com.sun.istack.NotNull;
import fr.finaxys.tutorials.utils.*;
import fr.finaxys.tutorials.utils.avro.models.*;
import fr.univlille1.atom.trace.TraceType;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import v13.*;
import v13.Day;
import v13.Order;
import v13.agents.Agent;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class AvroInjector implements AtomDataInjector {
	private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
			.getLogger(AtomDataInjector.class.getName());
	private final AtomConfiguration atomConf;
	private Configuration conf;
	private FileSystem fileSystem;
	private String destHDFS;
	private TimeStampBuilder tsb;
    private int dayGap;
    private DataFileWriter<VRecord> fileWriter ;

	public AvroInjector(@NotNull AtomConfiguration atomConf) throws Exception {
		this.atomConf = atomConf;
		this.destHDFS = atomConf.getAvroHDFSDest();
		this.conf = new Configuration();
		this.conf.addResource(new Path(atomConf.getHadoopConfCore()));
		this.conf.addResource(new Path(atomConf.getHadoopConfHdfs()));
		this.conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		this.conf.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());
        this.dayGap = atomConf.getDayGap();
	}

    public AvroInjector(@NotNull AtomConfiguration atomConf,Configuration conf) throws Exception {
        this.atomConf = atomConf;
        this.destHDFS = atomConf.getAvroHDFSDest();
        this.conf = conf;
        this.dayGap = atomConf.getDayGap();
    }


    public void createRecord()  {
        Schema schema = VRecord.getClassSchema() ;
        DatumWriter<VRecord> orderDatumWriter = new SpecificDatumWriter<>(schema) ;
        fileWriter = new DataFileWriter<VRecord>(orderDatumWriter);
        FSDataOutputStream file = null;
        try {
            file = fileSystem.create(new Path(destHDFS));
            fileWriter.create(schema, file);
        } catch (IOException e) {
            LOGGER.severe("can't create record for avro injection : "+e.getMessage());
            throw new HadoopTutorialException();
        }

	}

	// one schema for each agent ?

	@Override
	public void createOutput() {
		try {
			LOGGER.info("Create output ...");
            fileSystem = FileSystem.get(conf);
            //construct dataRecords
            createRecord();
		} catch (IOException e) {
            throw new HadoopTutorialException("Cannot create Avro output , verify if hdfs-site.xml path is good (check properties.txt)", e);
        }
	}

	@Override
	public void sendAgent(Agent a, Order o, PriceRecord pr) {
		try {
            //put agent fields
            VRecord record = new VRecord() ;
            record.setType(TraceType.Agent.name());
            fr.finaxys.tutorials.utils.avro.models.Agent agent = new fr.finaxys.tutorials.utils.avro.models.Agent() ;
            agent.setTrace(TraceType.Agent.name());
            agent.setAgentName(a.name);
            agent.setObName(o.obName);
            agent.setCash( a.cash);
            agent.setExecuted(pr.quantity);
            agent.setPrice(pr.price);
            if (o.getClass().equals(LimitOrder.class)) {
                agent.setDirection(((LimitOrder) o).direction+"");
                record.setTimestamp( pr.timestamp); // pr.timestamp
                agent.setOrderExtId(o.extId);
            }
            //append agent
            record.setAgent(agent);
			fileWriter.append(record);
		} catch (IOException e) {
			throw new HadoopTutorialException("Cannot create Avro output", e);
		}
	}

	@Override
	public void sendPriceRecord(PriceRecord pr, long bestAskPrice,
			long bestBidPrice) {
		try {
            long ts = tsb.nextTimeStamp();
            VRecord record = new VRecord() ;
            //put price fields 
            record.setType(TraceType.Price.name());
            Price price = new Price();
            price.setTrace(TraceType.Price.name());
            price.setObName(pr.obName);
            price.setPrice(pr.price);
            price.setExecuted(pr.quantity);
            price.setDir(pr.dir+"");
            price.setOrder1(pr.extId1);
            price.setOrder2(pr.extId2);
            price.setBestAsk(bestAskPrice);
            price.setBestBid(bestBidPrice);
            record.setTimestamp((pr.timestamp > 0 ? pr.timestamp : ts));
            record.setPrice(price);
            //append price 
            fileWriter.append(record);
		} catch (IOException e) {
            throw new HadoopTutorialException("failed write Price...", e);
		}
	}

	@Override
	public void sendAgentReferential(List<AgentReferentialLine> referencial) {
        for (AgentReferentialLine agent : referencial) {
            long ts = tsb.nextTimeStamp();
            VRecord record = new VRecord();
            record.setType("AgentReferential");
            //put agent fields
            AgentReferential agentRef = new AgentReferential();
            agentRef.setTrace("AgentReferential");
            agentRef.setAgentRefId(agent.agentRefId);
            agentRef.setAgentName(agent.agentName);
            agentRef.setIsMarketMaker(Boolean.toString(agent.isMarketMaker));
            agentRef.setDetails(agent.details);
            record.setTimestamp(ts);
            record.setAgentRef(agentRef);
            //append agent
            try {
                fileWriter.append(record);
            } catch (IOException e) {
                throw new HadoopTutorialException("failed write agentRef...", e);
            }
        }
	}

	@Override
	public void sendOrder(Order o) {
		try {
            VRecord record = new VRecord();
            record.setType(TraceType.Order.name());
            //put data fields
            fr.finaxys.tutorials.utils.avro.models.Order order = new fr.finaxys.tutorials.utils.avro.models.Order();
            order.setTrace(TraceType.Order.name());
            order.setObName(o.obName);
            order.setSender(o.sender.name);
            order.setExtId(o.extId);
            order.setType(o.type + "");
            order.setId(o.id);
            record.setTimestamp(o.timestamp);
            if (o.getClass().equals(LimitOrder.class)) {
                LimitOrder lo = (LimitOrder) o;
                order.setQuantity(lo.quantity);
                order.setDirection(lo.direction + "");
                order.setPrice(lo.price);
                order.setValidity(lo.validity);
            }
            record.setOrder(order);
            //append data
            fileWriter.append(record);
		} catch (IOException e) {
            throw new HadoopTutorialException("failed write order...", e);
		}
	}

	@Override
	public  void sendTick(Day day, Collection<OrderBook> orderbooks) {
		try {
            long ts = tsb.nextTimeStamp();
            for (OrderBook ob : orderbooks) {
                VRecord record = new VRecord();
                record.setType(TraceType.Tick.name());
                //put tick fields
                Tick tick = new Tick();
                tick.setTrace(TraceType.Tick.name());
                tick.setNumTick(day.currentTick());
                tick.setNumDay(day.number + dayGap);
                tick.setObName(ob.obName);
                record.setTimestamp(ts);
                if (!ob.ask.isEmpty()) {
                    tick.setBestAsk(ob.ask.last().price);
                }
                else tick.setBestAsk(0l);
                if (!ob.bid.isEmpty()) {
                    tick.setBestBid( ob.bid.last().price);
                }
                else  tick.setBestBid(0l);
                if (ob.lastFixedPrice != null) {
                    tick.setLastFixedPrice(ob.lastFixedPrice.price);
                }
                else tick.setLastFixedPrice(0l);
                //append tick
                record.setTick(tick);
                fileWriter.append(record);
            }
		} catch (IOException e) {
            throw new HadoopTutorialException("failed write Tick...", e);
		}
	}

	@Override
	public void sendDay(int nbDays, Collection<OrderBook> orderbooks) {
		try {
            long ts = tsb.nextTimeStamp();
            for (OrderBook ob : orderbooks) {
                VRecord record = new VRecord();
                record.setType(TraceType.Day.name());
                //put day fields
                fr.finaxys.tutorials.utils.avro.models.Day day = new fr.finaxys.tutorials.utils.avro.models.Day() ;
                day.setTrace(TraceType.Day.name());
                day.setNumDay(nbDays + dayGap);
                day.setObName(ob.obName);
                day.setFirstFixedPrice(ob.firstPriceOfDay);
                day.setLowestPrice(ob.lowestPriceOfDay);
                day.setHighestPrice(ob.highestPriceOfDay);
                long price = 0;
                if (ob.lastFixedPrice != null) {
                    price = ob.lastFixedPrice.price;
                }
                day.setLastFixedPrice(price);
                day.setNbPricesFixed(ob.numberOfPricesFixed);
                record.setTimestamp(ts);
                record.setDay(day);
                //append day
                fileWriter.append(record);
			}
		} catch (IOException e) {
            throw new HadoopTutorialException("failed write Day...", e);
		}
	}

	@Override
	public void sendExec(Order o) {
		try {
            long ts = tsb.nextTimeStamp();
            VRecord record = new VRecord();
            record.setType(TraceType.Exec.name());
            //put exec fields
            Exec exec = new Exec();
            exec.setTrace(TraceType.Exec.name());
            exec.setSender(o.sender.name);
            exec.setExtId(o.extId);
            record.setTimestamp(ts);
            record.setExec(exec);
            //append exec
            fileWriter.append(record);
		} catch (IOException e) {
            throw new HadoopTutorialException("failed write exec...", e);
		}
	}

	@Override
	public void closeOutput() {
		try {
            // destruct dataFileWriters
            fileWriter.close();
            fileSystem.close();
            LOGGER.info("Close connection ");
            LOGGER.info("Data imported ");
		} catch (IOException e) {
            throw new HadoopTutorialException("failed close fileSystem...", e);
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

    public int getDayGap() {
        return dayGap;
    }

    public void setDayGap(int dayGap) {
        this.dayGap = dayGap;
    }
}
