package fr.finaxys.tutorials.utils.hbase;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import fr.univlille1.atom.trace.TraceType;
import static fr.finaxys.tutorials.utils.hbase.AtomHBaseHelper.*;

public class AgentPosition extends Configured implements Tool {
	
	private Configuration conf;
	
	public static final String AP_RESULT_TABLE = "AgentPosition";
	public static final String AP_RESULT_CF = "cf";
	public static final String AP_RESULT_QUAL = "count";
	
	private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
			.getLogger(AgentPosition.class.getName());
	
	public AgentPosition() {
	}
	
	public AgentPosition(Configuration conf) {
		this.conf = conf;
	}

	public static class AgentPositionMapper extends TableMapper<Text, IntWritable> {

		private byte[] columnFamily = null;
		
		// private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		@Override
	       protected void setup(Context context)
	         throws IOException, InterruptedException {
	         String column = context.getConfiguration().get("conf.trace.cf");
	         byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
	         columnFamily = colkey[0];
	         LOGGER.log(Level.INFO, "Column Family is "+columnFamily);
	       }

		@Override
		protected void map(ImmutableBytesWritable rowkey, Result result,
				Context context) throws IOException, InterruptedException {

			byte[] rowKey = result.getRow();
			
			TraceType type = lookupType(rowKey);
			LOGGER.log(Level.INFO, "Mapping "+rowKey + " type " + type);
			if (TraceType.Order.equals(type)) {
				byte[] q = result.getValue(columnFamily, Q_QUANTITY);
				byte[] a = result.getValue(columnFamily, Q_SENDER);
				byte[] ob = result.getValue(columnFamily, Q_OB_NAME);

				if (a == null) {
					LOGGER.log(Level.WARNING, "Agent Name for row "+rowKey + " is null");
					return;
				}
				int quantity = Bytes.toInt(q);
				String agentName = Bytes.toString(a);
				String orderBook = Bytes.toString(ob);
				if (quantity == 0) {
					LOGGER.log(Level.FINE, "Quantity = 0 for "+rowKey);
					return;
				}	
				String name = agentName + "-" + orderBook;
				word.set(name);
				context.write(word, new IntWritable(quantity));
			}

		}
	}
	
	public static class AgentPositionReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable>  {

	 	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    		int i = 0;
	    		for (IntWritable val : values) {
	    			i += val.get();
	    		}
	    		Put put = new Put(Bytes.toBytes(key.toString()));
	    		put.addColumn(Bytes.toBytes(AP_RESULT_CF), Bytes.toBytes(AP_RESULT_QUAL), Bytes.toBytes(i));
	    		context.write(null, put);
	   	}
	}

	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    String sDate = args[0];
	    
	    DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
	    Date date = df.parse(sDate);
	    Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		long minStamp = cal.getTimeInMillis();
		cal.add(Calendar.DAY_OF_YEAR, 1);
		long maxStamp = cal.getTimeInMillis();
		LOGGER.log(Level.INFO, "agentPosition Date : " +date + ", Min TimeStamp : "+ minStamp + ", Max TimeStamp : "+ maxStamp);
		String[] args2 = {Long.toString(minStamp),Long.toString(maxStamp),
				args[1], args[2]};
		
	    ToolRunner.run(new AgentPosition(conf), args2);
	 }

	@Override
	public int run(String[] arg0) throws Exception {
		
		assert (arg0.length == 4);
		
		LOGGER.log(Level.INFO, "Agent Position run args"+arg0);
		
		//Configuration conf = HBaseConfiguration.create();
		
		conf.set("conf.trace.cf", arg0[3]);
		
		Job job = Job.getInstance(conf, "Agent Position");
		job.setJarByClass(AgentPosition.class);

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs

		List<Filter> myList = new ArrayList<Filter>();
		Filter f1 = new SingleColumnValueFilter(Bytes.toBytes(arg0[3]), Q_TIMESTAMP, 
				  CompareOp.GREATER_OR_EQUAL,
				  new LongComparator(Long.parseLong(arg0[0])));
		myList.add(f1);
		Filter f2 = new SingleColumnValueFilter(Bytes.toBytes(arg0[3]), Q_TIMESTAMP, 
				  CompareOp.LESS,
				  new LongComparator(Long.parseLong(arg0[1])));
		myList.add(f2);
		FilterList myFilterList =
				new FilterList(FilterList.Operator.MUST_PASS_ALL, myList);
		scan.setFilter(myFilterList);
		
		TableMapReduceUtil.initTableMapperJob(
				arg0[2], 
				scan,
				AgentPositionMapper.class,
				Text.class,
				IntWritable.class,
				job);

		TableMapReduceUtil.initTableReducerJob(
				AP_RESULT_TABLE,        // output table
				AgentPositionReducer.class,    // reducer class
				job);
		job.setNumReduceTasks(1);   // at least one, adjust as required

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}
}
