package fr.finaxys.tutorials.utils.parquet;


import fr.finaxys.tutorials.utils.avro.models.VRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvroParquetConverterMapper extends
        Mapper<AvroKey<VRecord>, NullWritable, Void, VRecord> {

    @Override
    protected void map(AvroKey<VRecord> key, NullWritable value,
                       Context context) throws IOException, InterruptedException {
        context.write(null, key.datum());
    }
}