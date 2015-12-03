package fr.finaxys.tutorials.utils.parquet;

import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.AtomDataInjector;
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

    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(AtomDataInjector.class.getName());
    private Configuration conf = null ;
    static private String FILE_PATH = "/part-m-00000.snappy.parquet" ;

    private AtomConfiguration atomConfiguration ;

    public ParquetReader(AtomConfiguration atomConfiguration,Configuration conf){
        this.atomConfiguration = atomConfiguration ;
        this.conf = conf ;
    }
    public ParquetReader(AtomConfiguration atomConfiguration){
        this.atomConfiguration = atomConfiguration ;
    }

    public void setConfiguration(Configuration conf){
        this.conf = conf ;
    }

    public static class ReadRequestMap extends Mapper<LongWritable, Group, NullWritable, Text> {
        @Override
        public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
            NullWritable outKey = NullWritable.get();
            context.write(outKey, new Text(value.toString().replaceAll("\n  ",";").split("\n")[1].replaceAll(" ","")));
        }
    }


    public int run(String[] args) throws Exception {
        AtomConfiguration atom = new AtomConfiguration() ;
        if(conf == null){
            conf = new Configuration();
            conf.addResource(new Path(atom.getHadoopConfHdfs()));
            conf.reloadConfiguration();
        }
        Path inputPath = new Path(args[0]+FILE_PATH);
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

    public void read(String outputFilePath){
        try {
            String[] otherArgs = {atomConfiguration.getParquetHDFSDest(),outputFilePath} ; // parquet file path into hdfs , output file
            int res = ToolRunner.run(conf, new ParquetReader(atomConfiguration,this.conf), otherArgs);
            //System.exit(res);
        } catch (Exception e) {
            LOGGER.severe("failed to load hdfs conf..." + e.getMessage());
            //System.exit(255);
        }
    }

    public static void main(String[] args) throws Exception {
        ParquetReader reader = new ParquetReader(new AtomConfiguration());
        reader.read(args[0]);
    }
}