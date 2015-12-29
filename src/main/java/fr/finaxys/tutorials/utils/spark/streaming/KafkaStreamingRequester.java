package fr.finaxys.tutorials.utils.spark.streaming;

import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.HadoopTutorialException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by finaxys on 12/22/15.
 */
public class KafkaStreamingRequester {
    private static JavaSparkContext jsc;
    private static AtomConfiguration atom = new AtomConfiguration();
    private static int i = 0 ;
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaStreamingRequester.class);

    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("kafkaStreaming");
        try{
            jsc = new JavaSparkContext(sparkConf);
            LOGGER.info("spark started with distributed mode");
        }catch(Exception e){
            LOGGER.info("spark can't start with distributed mode");
            sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafkaStreaming");
            jsc = new JavaSparkContext(sparkConf);
        }
        JavaStreamingContext jssc = new JavaStreamingContext(jsc,
                Durations.seconds(2));
        Map<String, Integer> topics  = new HashMap<>();
        topics.put(atom.getKafkaTopic(),new Integer(1));
        JavaPairReceiverInputDStream<String, String> kafkaStream = KafkaUtils.createStream(jssc,atom.getKafkaQuorum(),"groupid",topics);

        kafkaStream.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {

            private static final long serialVersionUID = -6487126638165154032L;

            @Override
            public Void call(JavaPairRDD<String, String> rdd) throws Exception {
                LOGGER.info("data count = "+rdd.count());
                rdd.map(new Function<Tuple2<String,String>, Object>() {
                    @Override
                    public Put call(Tuple2<String, String> data) throws Exception {
                        return null;
                    }
                });
                return null;
            }
        });


        kafkaStream.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {

            private static final long serialVersionUID = -6487126638165154032L;

            @Override
            public Void call(JavaPairRDD<String, String> rdd) throws Exception {

                // create connection with HBase
                final Configuration config = new Configuration();
                try {
                    config.addResource(new Path(atom.getHbaseConfHbase()));
                    config.reloadConfiguration();
                    HBaseAdmin.checkHBaseAvailable(config);
                    LOGGER.info("HBase is running!");
                }
                catch (Exception e) {
                    LOGGER.error("HBase is not running!" + e.getMessage());
                    throw new HadoopTutorialException(e.getMessage());
                }
                config.set(TableInputFormat.INPUT_TABLE, atom.getTableName());

                final Job newAPIJobConfiguration1 ;
                try {
                    newAPIJobConfiguration1 = Job.getInstance(config);
                } catch (IOException e) {
                    LOGGER.error("can't create mapRed conf");
                    throw new HadoopTutorialException(e.getMessage());
                }
                newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, atom.getTableName());
                newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);



                JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = rdd.mapToPair(new PairFunction<Tuple2<String, String>, ImmutableBytesWritable, Put>() {
                    @Override
                    public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                        Put put = new Put(Bytes.toBytes("row" + i));
                        i++;
                        put.addColumn(atom.getCfName(), Bytes.toBytes("value"), Bytes.toBytes(stringStringTuple2._2()));
                        if(i%1000 == 0) LOGGER.info("nb of data inserted " + i);
                        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
                    }
                });

                hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
                return null;
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
