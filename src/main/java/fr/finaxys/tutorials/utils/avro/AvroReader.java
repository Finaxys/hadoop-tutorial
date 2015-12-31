package fr.finaxys.tutorials.utils.avro;

import fr.finaxys.tutorials.utils.AtomDataInjector;
import fr.finaxys.tutorials.utils.avro.models.VRecord;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by finaxys on 11/18/15.
 */
public class AvroReader {

    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(AtomDataInjector.class.getName());
	
    private Configuration conf;
    private String avroHDFSDest;

    public AvroReader(String hadoopConfCore, String hadoopConfHdfs, String avroHDFSDest) {
        
        this.conf = new Configuration();
        this.conf.addResource(new Path(hadoopConfCore));
        this.conf.addResource(new Path(hadoopConfHdfs));
        this.conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        this.conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName());
        
        this.avroHDFSDest = avroHDFSDest;
    }

    public AvroReader(Configuration conf, String avroHDFSDest) {
        this.conf = conf ;
        this.avroHDFSDest = avroHDFSDest;
    }
    
    public AvroReader(Configuration conf) {
        this.conf = conf ;
    }


    public  List<VRecord> scanRecords(int max) {
        try {
            List<VRecord> result = new ArrayList<VRecord>();
            DatumReader<VRecord> datumReader = new SpecificDatumReader<>(VRecord.getClassSchema());
            SeekableInput file = new FsInput(new Path(avroHDFSDest), conf);
            DataFileReader<VRecord> dataFileReader = new DataFileReader<VRecord>(file, datumReader);
            VRecord exec = null;
            int i=0 ;
            while (dataFileReader.hasNext() && i<max) {
                result.add(dataFileReader.next(exec));
                i++ ;
            }
            file.close();
            dataFileReader.close();
            return result;
        } catch (IOException e) {
            LOGGER.severe( "failed read Records..." + e.getMessage());
            return null ;
        }
    }

	public String getAvroHDFSDest() {
		return avroHDFSDest;
	}

	public void setAvroHDFSDest(String avroHDFSDest) {
		this.avroHDFSDest = avroHDFSDest;
	}
}
