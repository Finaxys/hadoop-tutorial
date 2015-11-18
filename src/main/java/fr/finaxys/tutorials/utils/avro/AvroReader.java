package fr.finaxys.tutorials.utils.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * Created by finaxys on 11/18/15.
 */
public class AvroReader {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/tmp/configuration.xml"));
        conf.reloadConfiguration();
        //FileSystem fileSystem = FileSystem.get(conf);
        Schema schema = new Schema.Parser().parse(new File("/home/finaxys/dev/hadoop-tutorial/avro/price.avsc"));
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        SeekableInput file = new FsInput(new Path("/priceFile"),conf);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }
}
