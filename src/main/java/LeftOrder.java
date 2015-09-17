import fr.univlille1.atom.trace.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Hello world!
 * 
 */
public class LeftOrder extends Configured implements Tool {
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {

		// private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String trace = value.toString();
			if (TraceUtils.isTrace(TraceType.Order, trace)) {
				OrderTrace order = (OrderTrace) TraceUtils.buildTrace(trace);
				word.set(order.getSender() + "-" + order.getIdentifier());
				context.write(word, new Text(""));
			}
			else if (TraceUtils.isTrace(TraceType.Exec, trace)) {
				ExecTrace exec = (ExecTrace) TraceUtils.buildTrace(trace);
				word.set(exec.getOrderIdentifier());
				context.write(word, new Text(""));
			}
		}
	}

	public static class NotMatchedReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			long sum = 0;

			for (Text val : values) {
				++sum;
			}

			if (sum == 1)
				context.write(key, new Text(""));
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Agent Position");
		job.setJarByClass(LeftOrder.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(NotMatchedReducer.class);
		job.setReducerClass(NotMatchedReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.submit();
	    return 0;
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    ToolRunner.run(new LeftOrder(), otherArgs);
	 }
	
}
