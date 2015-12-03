package fr.finaxys.tutorials.utils.avro;

import fr.finaxys.tutorials.utils.AtomConfiguration;
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

    private final AtomConfiguration atomConf;
    private Configuration conf;
    private String destHDFS;
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(AtomDataInjector.class.getName());

    public AvroReader(AtomConfiguration atomConf) {
        this.atomConf = atomConf;
        this.destHDFS = atomConf.getAvroHDFSDest();
        this.conf = new Configuration();
        this.conf.addResource(new Path(atomConf.getHadoopConfCore()));
        this.conf.addResource(new Path(atomConf.getHadoopConfHdfs()));
        this.conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        this.conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    public AvroReader(AtomConfiguration atomConf,Configuration conf) {
        this.atomConf = atomConf;
        this.destHDFS = atomConf.getAvroHDFSDest();
        this.conf = conf ;
    }


    public  List<VRecord> scanRecords(int max) {
        try {
            List<VRecord> result = new ArrayList<VRecord>();
            DatumReader<VRecord> datumReader = new SpecificDatumReader<>(VRecord.getClassSchema());
            SeekableInput file = new FsInput(new Path(destHDFS), conf);
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
}
