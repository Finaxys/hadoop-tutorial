package fr.finaxys.tutorials.utils.parquet;


import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.avro.models.VRecord;
import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.logging.Level;


public class AvroParquetConverter extends Configured implements Tool {

    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(AvroParquetConverter.class.getName());
    private Configuration configuration= null ;

    public void setConfiguration(Configuration configuration){
        this.configuration = configuration ;
    }

    public int run(String[] args) throws Exception {
        AtomConfiguration atom = new AtomConfiguration();
        Path inputPath = new Path(atom.getDestHDFS());
        Path outputPath = new Path(args[0]);
        Configuration conf;
        if (this.configuration == null){
            conf = new Configuration();
            conf.addResource(new Path(atom.getHadoopConfHdfs()));
            conf.reloadConfiguration();
        }
        else {
            conf = this.configuration ;
        }
        Job job = Job.getInstance(conf, "Parquet Conversion");
        job.setJarByClass(getClass());
        Schema avroSchema = VRecord.getClassSchema();
        System.out.println(new AvroSchemaConverter().convert(avroSchema).toString());
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, outputPath);
        AvroParquetOutputFormat.setSchema(job, avroSchema);
        AvroParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        AvroParquetOutputFormat.setCompressOutput(job, true);
        AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);
        job.setMapperClass(AvroParquetConverterMapper.class);
        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public boolean convert(String outPutPath){
        String[] otherArgs = {outPutPath} ; // args[0]=ParquetOutPutDir
        int exitCode = 0;
        boolean success = false;
        try {
            exitCode = ToolRunner.run(new AvroParquetConverter(), otherArgs);
            success = true;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Can't run map reduce job", e);
        }
        finally {
            System.exit(exitCode);
            return success ;
        }
    }

    public static void main(String[] args) throws Exception {
        if(args.length != 0) {
            AtomConfiguration atomConf = new AtomConfiguration() ;
            AvroParquetConverter converter = new AvroParquetConverter();
            converter.convert(args[0]); //ParquetOutPutDir
        }
        else {
            LOGGER.info("Please set output file path .");
        }
    }

}