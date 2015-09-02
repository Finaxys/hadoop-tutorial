

import java.io.IOException;

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

import fr.univlille1.atom.trace.AgentTrace;
import fr.univlille1.atom.trace.TraceType;
import fr.univlille1.atom.trace.TraceUtils;

/**
 * Hello world!
 * 
 */
public class AgentPosition extends Configured implements Tool {
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, LongWritable> {

		// private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String trace = value.toString();
			if (TraceUtils.isTrace(TraceType.Agent, trace)) {
				AgentTrace agent = (AgentTrace) TraceUtils.buildTrace(trace);
				word.set(agent.getName() + "-" + agent.getAssetName());
				context.write(word, new LongWritable(agent.getAssetQuantity()));
			}
		}
	}

	public static class LongSumReducer extends
			Reducer<Text, IntWritable, Text, LongWritable> {

		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Agent Position");
		job.setJarByClass(AgentPosition.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.submit();
	    return 0;
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    ToolRunner.run(new AgentPosition(), otherArgs);
	 }
	
}
