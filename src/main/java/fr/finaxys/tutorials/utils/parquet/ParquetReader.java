package fr.finaxys.tutorials.utils.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.ExampleInputFormat;

import java.io.IOException;




/**
 * Created by finaxys on 11/20/15.
 */
public class ParquetReader extends Configured implements Tool {


    /*
     * Read a Parquet record, write a CSV record
     */
    public static class ReadRequestMap extends Mapper<LongWritable, Group, NullWritable, Text> {
        @Override
        public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
            NullWritable outKey = NullWritable.get();
            System.out.println(new Text(value.toString()));
            context.write(outKey, new Text(value.toString()));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration() ;
        conf.addResource("/tmp/configuration.xml");
        conf.reloadConfiguration();
        Path inputPath = new Path("hdfs://localhost:52896"+args[0]);
        Path outputPath = new Path(args[1]);

        Job job = Job.getInstance(conf, "Parquet reader");
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ReadRequestMap.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(ExampleInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        try {
            String[] otherArgs = {"/priceParquetp/part-m-00000.snappy.parquet","/tmp/newg"} ;
            Configuration conf = new Configuration() ;
            conf.addResource("/tmp/configuration.xml");
            conf.reloadConfiguration();
            int res = ToolRunner.run(conf, new ParquetReader(), otherArgs);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }
}