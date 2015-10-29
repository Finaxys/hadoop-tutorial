package fr.tutorials.utils.avro;

import com.sun.istack.NotNull;
import fr.tutorials.utils.AtomConfiguration;
import fr.tutorials.utils.AtomDataInjector;
import fr.tutorials.utils.hbase.AgentReferentialLine;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import v13.Day;
import v13.Order;
import v13.OrderBook;
import v13.PriceRecord;
import v13.agents.Agent;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class AvroInjector implements AtomDataInjector
{
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(AtomDataInjector.class.getName());

    private final AtomConfiguration atomConf;
    private Configuration conf;
    private Schema schema;
    private FileSystem fileSystem;
    private String destHDFS;
    private Path pathDestHDFS;
    private String pathAvroFile;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private GenericRecord genericRecord;

    public AvroInjector(@NotNull AtomConfiguration atomConf) throws Exception
    {
        this.atomConf = atomConf;
        this.destHDFS = atomConf.getDestHDFS();
        this.pathAvroFile = atomConf.getPathAvro();
        String pathSchema = atomConf.getAvroSchema();
        //boolean isAvro = atomConf.isOutAvro();
        this.conf = new Configuration();
        this.conf.addResource(new Path(atomConf.getPathCore()));
        this.conf.addResource(new Path(atomConf.getPathSite()));
        this.conf.addResource(new Path(atomConf.getPathHDFS()));
        this.conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        this.conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        this.schema = new Schema.Parser().parse(new File(pathSchema));
    }

    public void sendToHDFS(Path path) throws IOException
    {
        fileSystem = FileSystem.get(conf);
        pathDestHDFS = new Path(destHDFS);

        try
        {
            fileSystem.copyFromLocalFile(false, true, path, pathDestHDFS);
        }
        catch (Exception e)
        {
            LOGGER.severe("Exception : " + e);
        }
        finally
        {
            fileSystem.close();
        }
    }

    //one schema for each agent ?

    @Override
    public void createOutput() 
    {
    	try {
	        LOGGER.info("Create output ...");
	        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
	        this.dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
	        File file = new File(pathAvroFile);
	        this.dataFileWriter.create(schema, file);
	        this.genericRecord = new GenericData.Record(schema);
    	} catch (IOException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		}
    }

    @Override
    public void sendAgent(Agent a, Order o, PriceRecord pr)
    {
    	 try {
	        genericRecord.put("type", "Agent");
	        genericRecord.put("name", a.name);
	        genericRecord.put("cash", o.obName);
	        genericRecord.put("assetName", a.cash);
	        genericRecord.put("assetQuantity", pr.quantity);
	        genericRecord.put("currentPrice", pr.price);
			dataFileWriter.append(genericRecord);
        
    	 } catch (IOException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		}
    }

    @Override
    public void sendPriceRecord(PriceRecord pr, long bestAskPrice, long bestBidPrice)
    {
    	try {
	        genericRecord.put("type", "Price");
	        genericRecord.put("orderBook", pr.obName);
	        genericRecord.put("price", pr.price);
	        genericRecord.put("quantity", pr.quantity);
	        genericRecord.put("direction", pr.dir);
	        genericRecord.put("initiatingOrderIdentifier", pr.extId1);
	        genericRecord.put("fullfillingOrderIdentifier", pr.extId2);
	        genericRecord.put("currentBestAsk", bestAskPrice);
	        genericRecord.put("currentBestBid", bestBidPrice);
	        dataFileWriter.append(genericRecord);
    	} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    @Override
    public void sendAgentReferential(List<AgentReferentialLine> referencial)
    {
        genericRecord.put("", "");
    }

    @Override
    public void sendOrder(Order o) 
    {
    	try {
	        genericRecord.put("type", "Order");
	        genericRecord.put("orderBook", "");
	        genericRecord.put("sender", "");
	        genericRecord.put("identifier", "");
	        genericRecord.put("nature", "");
	        genericRecord.put("direction", "");
	        genericRecord.put("price", "");
	        genericRecord.put("quantity", "");
	        genericRecord.put("validity", "");
	        dataFileWriter.append(genericRecord);
        } catch (IOException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		}
    }

    @Override
    public void sendTick(Day day, Collection<OrderBook> orderbooks) 
    {
    	try {
	        genericRecord.put("type", "Tick");
	        genericRecord.put("number", "");
	        genericRecord.put("orderBook", "");
	        genericRecord.put("bestAsk", "");
	        genericRecord.put("bestBid", "");
	        genericRecord.put("lastFixedPriced", "");
	        dataFileWriter.append(genericRecord);
    	} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    @Override
    public void sendDay(int nbDays, Collection<OrderBook> orderbooks) 
    {
    	try {
	        for (OrderBook ob : orderbooks)
	        {
	            genericRecord.put("type", "Day");
	            genericRecord.put("number", nbDays + atomConf.getDayGap());
	            genericRecord.put("assetName", ob.obName);
	            genericRecord.put("lastFixedPrice1", ob.firstPriceOfDay);
	            genericRecord.put("lastFixedPrice2", ob.lowestPriceOfDay);
	            genericRecord.put("lastFixedPrice3", ob.highestPriceOfDay);
	            long price = 0;
	            if (ob.lastFixedPrice != null)
	                price = ob.lastFixedPrice.price;
	            genericRecord.put("lastFixedPrice4", price);
	            genericRecord.put("fixedPriceCount", ob.numberOfPricesFixed);
	            dataFileWriter.append(genericRecord);
	        }
    	} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    @Override
    public void sendExec(Order o) 
    {
    	try {
	        genericRecord.put("type", "Exec");
	        genericRecord.put("orderIdentifier", o.extId);
	        dataFileWriter.append(genericRecord);
    	} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    @Override
    public void close() 
    {
    	try {
	        Path pathAvro = new Path(pathAvroFile);
	        sendToHDFS(pathAvro);
    	} catch (IOException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		}
    }
}
