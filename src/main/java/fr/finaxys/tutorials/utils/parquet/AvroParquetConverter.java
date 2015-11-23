package fr.finaxys.tutorials.utils.parquet;


import fr.finaxys.tutorials.utils.AtomConfiguration;
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

import java.io.File;


public class AvroParquetConverter extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        AtomConfiguration atom = new AtomConfiguration() ;
        //Path schemaPath = new Path(args[0]);
        String type = args[1];
        Path inputPath = new Path(atom.getDestHDFS()+type+"File");
        Path outputPath = new Path(args[0]);
        Configuration conf = new Configuration();
        conf.addResource(new Path(atom.getHadoopConfHdfs()));
        conf.reloadConfiguration();

        Job job = Job.getInstance(conf, "Parquet Conversion");
        job.setJarByClass(getClass());


        //FileSystem fs = FileSystem.get(conf);
        //InputStream in = fs.open(schemaPath);
        Schema avroSchema = new Schema.Parser().parse(new File(atom.getAvroSchema()+"/"+type+"."+atom.getExtAvro()));

        System.out.println(new AvroSchemaConverter().convert(avroSchema).toString());

        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, outputPath);
        AvroParquetOutputFormat.setSchema(job, avroSchema);
        AvroParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        AvroParquetOutputFormat.setCompressOutput(job, true);

    /* Impala likes Parquet files to have only a single row group.
     * Setting the block size to a larger value helps ensure this to
     * be the case, at the expense of buffering the output of the
     * entire mapper's split in memory.
     *
     * It would be better to set this based on the files' block size,
     * using fs.getFileStatus or fs.listStatus.
     */
        AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);

        job.setMapperClass(AvroParquetConverterMapper.class);
        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        String[] otherArgs = {"/priceParquet","price"} ; // args[0]=ParquetOutPutDir args[1]=ModelType
        int exitCode = ToolRunner.run(new AvroParquetConverter(), otherArgs);
        System.exit(exitCode);
    }

}