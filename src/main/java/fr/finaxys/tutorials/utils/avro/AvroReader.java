package fr.finaxys.tutorials.utils.avro;

import fr.finaxys.tutorials.utils.AtomConfiguration;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
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
    private String avroExt ;
    private String pathSchema ;

    public AvroReader(AtomConfiguration atomConf) {
        this.atomConf = atomConf;
        this.destHDFS = atomConf.getDestHDFS();

        // boolean isAvro = atomConf.isOutAvro();
        this.conf = new Configuration();
        this.conf.addResource(new Path(atomConf.getHadoopConfCore()));
        this.conf.addResource(new Path(atomConf.getHadoopConfHdfs()));
        this.conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        this.conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName());
        this.pathSchema = atomConf.getAvroSchema();
        this.avroExt = atomConf.getExtAvro() ;
    }

    public AvroReader(AtomConfiguration atomConf,Configuration conf) {
        this.atomConf = atomConf;
        this.destHDFS = atomConf.getDestHDFS();
        this.conf = conf ;
        this.pathSchema = atomConf.getAvroSchema();
        this.avroExt = atomConf.getExtAvro() ;
    }


    public <T extends SpecificRecordBase> List<T> scanRecords(String type) {
        try {
            List<T> result = new ArrayList<T>();
            Schema schema = new Schema.Parser().parse(new File(pathSchema + "/" + type + "." + avroExt));
            DatumReader<T> datumReader = new SpecificDatumReader<>(schema);
            SeekableInput file = new FsInput(new Path(destHDFS+type+"File"), conf);
            DataFileReader<T> dataFileReader = new DataFileReader<T>(file, datumReader);
            T exec = null;
            while (dataFileReader.hasNext()) {
                result.add(dataFileReader.next(exec));
            }
            file.close();
            dataFileReader.close();
            return result;
        } catch (IOException e) {
            e.printStackTrace();
            return null ;
        }
    }
}
