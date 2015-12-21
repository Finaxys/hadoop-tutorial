package fr.finaxys.tutorials.utils.spark.streaming;

import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.hbase.HBaseAnalysis;
import fr.finaxys.tutorials.utils.spark.models.DataRow;
import fr.finaxys.tutorials.utils.spark.utils.Converter;
import fr.finaxys.tutorials.utils.spark.utils.RequestReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by finaxys on 12/4/15.
 */
public class HBaseStreamingRequester {
    private static final AtomConfiguration atomConfiguration= new AtomConfiguration();
    private static final String HBASE_SITE_PATH = atomConfiguration.getHbaseConfHbase();
    private static final String TABLE_NAME = atomConfiguration.getTableName();
    private static final byte[] COLUMN_FAMILY = atomConfiguration.getColumnFamily() ;
    public static final RequestReader requestReader= new RequestReader("spark-requests/hbase-streaming-request.sql");
    public static final String resultQualifier = "result";
    public static final int max = 10000 ;

    private static JavaSparkContext jsc;
    private static Configuration conf;
    private static Long lastTS;
    private static JavaRDD<DataRow> mainRDDs ;
    /**
     * @param args
     */
    public static void main(String[] args) {
        final String request = requestReader.readRequest();
        SparkConf sparkConf = new SparkConf().setAppName("sparkApplication");
        try{
            jsc = new JavaSparkContext(sparkConf);
        }catch(Exception e){
            sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkApplication");
            jsc = new JavaSparkContext(sparkConf);
        }

        JavaStreamingContext jssc = new JavaStreamingContext(jsc,
                Durations.seconds(2));
        JavaReceiverInputDStream<DataRow> jrids = jssc
                .receiverStream(new EmptyReceiver(StorageLevel.MEMORY_ONLY()));
        final Converter converter = new Converter(COLUMN_FAMILY);
        conf = HBaseConfiguration.create();
        conf.addResource(new Path(HBASE_SITE_PATH));
        conf.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);

        JavaDStream<DataRow> newDStream = jrids
                .transform(new Function<JavaRDD<DataRow>, JavaRDD<DataRow>>() {

                    private static final long serialVersionUID = 3461266141582186411L;

                    @Override
                    public JavaRDD<DataRow> call(JavaRDD<DataRow> arg0)
                            throws Exception {

                        if (lastTS != null) {
                            conf.set(TableInputFormat.SCAN_TIMERANGE_START, lastTS.toString());
                            conf.set(TableInputFormat.SCAN_TIMERANGE_END, Calendar.getInstance().getTime().getTime()+"");
                        }
                        conf.reloadConfiguration();

                        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc
                                .newAPIHadoopRDD(conf, TableInputFormat.class,
                                        ImmutableBytesWritable.class,
                                        Result.class);

                        JavaRDD<DataRow> mapped = hBaseRDD
                                .map(new Function<Tuple2<ImmutableBytesWritable, Result>, DataRow>() {

                                    private static final long serialVersionUID = -570690400768209214L;

                                    @Override
                                    public DataRow call(
                                            Tuple2<ImmutableBytesWritable, Result> tuple)
                                            throws Exception {
                                        DataRow dr = converter
                                                .convertTupleToDataRow(tuple);
                                        lastTS = tuple._2.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("Trace")).getTimestamp();
                                        lastTS++;
                                        return dr;
                                    }
                                });

                        if (mainRDDs == null ) {mainRDDs = mapped ;}
                        else {mainRDDs.union(mapped).distinct();}

                        return mainRDDs;
                    }
                });

        newDStream.foreachRDD(new Function<JavaRDD<DataRow>, Void>() {

            private static final long serialVersionUID = -6487126638165154032L;

            @Override
            public Void call(JavaRDD<DataRow> dr) throws Exception {
                SQLContext sqlContext = new SQLContext(jsc);
                DataFrame df = sqlContext.createDataFrame(dr, DataRow.class);
                df.registerTempTable("records");
                DataFrame df2 = sqlContext.sql(request);
                df2.show(max);

                // put data
                HBaseAnalysis analysis = new HBaseAnalysis();
                analysis.setTableName(TableName.valueOf(atomConfiguration.getSparkTableName()));
                analysis.setColumnFamily(atomConfiguration.getColumnFamily());
                analysis.setHbaseConfiguration(conf);
                analysis.openTable();
                Date date = new Date();
                Put p = new Put(Bytes.toBytes("r-"+date.getTime()));
                p.addColumn(atomConfiguration.getColumnFamily(),Bytes.toBytes(resultQualifier),Bytes.toBytes(df2.showString(max,false)));
                analysis.directPutTable(p);
                analysis.closeTable();

                return null;
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }

}
