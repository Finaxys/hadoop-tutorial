package fr.finaxys.tutorials.utils.parquet;


import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.HadoopTutorialException;
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

import java.io.IOException;
import java.util.logging.Level;


public class AvroParquetConverter extends Configured implements Tool {

    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(AvroParquetConverter.class.getName());
    private Configuration configuration;
    //private AtomConfiguration atom;
    private String avroHDFSDest;
    private String hadoopConfHdfs;
    private String parquetHDFSDest;

    public AvroParquetConverter() {
    }
    
    public AvroParquetConverter(Configuration configuration) {
    	this.configuration = configuration;
    }

    public void setConfiguration(Configuration configuration){
        this.configuration = configuration ;
    }

    public int run(String[] args) {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Configuration conf;
        if (this.configuration == null){
            conf = new Configuration();
            conf.addResource(new Path(getHadoopConfHdfs()));
            conf.reloadConfiguration();
        }
        else {
            conf = this.configuration ;
            conf.reloadConfiguration();
        }
        Job job = null;
        try {
            job = Job.getInstance(conf, "Parquet Conversion");
        } catch (IOException e) {
            LOGGER.severe("can't launch parquet conversion job : "+e.getMessage());
            throw new HadoopTutorialException();

        }
        job.setJarByClass(getClass());
        Schema avroSchema = VRecord.getClassSchema();
        try {
            FileInputFormat.addInputPath(job, inputPath);
        } catch (IOException e) {
            LOGGER.severe("can't read input avro file : ");
            throw new HadoopTutorialException();
        }
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, outputPath);
        AvroParquetOutputFormat.setSchema(job, avroSchema);
        AvroParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        AvroParquetOutputFormat.setCompressOutput(job, true);
        AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);
        job.setMapperClass(AvroParquetConverterMapper.class);
        job.setNumReduceTasks(0);
        int success = 0;
        try {
            success = job.waitForCompletion(true) ? 0 : 1;
        } catch (Exception e) {
            LOGGER.severe("can't wait for completion : "+e.getMessage());
            throw new HadoopTutorialException("exception while converting avro in parquet", e) ;
        }
        return success;
    }

    public boolean convert(){
        String[] otherArgs = {getAvroHDFSDest(), getParquetHDFSDest()} ;
        boolean success = false;
        try {
            ToolRunner.run(this, /*new AvroParquetConverter(this.configuration),*/ otherArgs);
            success = true;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Can't run map reduce job", e);
        }
        return success;
    }

    public static void main(String[] args)  {
    	AtomConfiguration atomConfiguration = AtomConfiguration.getInstance();
        AvroParquetConverter converter = new AvroParquetConverter();
        converter.setAvroHDFSDest(atomConfiguration.getAvroHDFSDest());
        converter.setHadoopConfHdfs(atomConfiguration.getHadoopConfHdfs());
        converter.setParquetHDFSDest(atomConfiguration.getParquetHDFSDest());
        converter.convert();
    }

	public String getAvroHDFSDest() {
		return avroHDFSDest;
	}

	public void setAvroHDFSDest(String avroHDFSDest) {
		this.avroHDFSDest = avroHDFSDest;
	}

	public String getHadoopConfHdfs() {
		return hadoopConfHdfs;
	}

	public void setHadoopConfHdfs(String hadoopConfHdfs) {
		this.hadoopConfHdfs = hadoopConfHdfs;
	}


	public String getParquetHDFSDest() {
		return parquetHDFSDest;
	}


	public void setParquetHDFSDest(String parquetHDFSDest) {
		this.parquetHDFSDest = parquetHDFSDest;
	}

}