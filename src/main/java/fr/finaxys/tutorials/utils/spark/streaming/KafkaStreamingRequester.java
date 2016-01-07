package fr.finaxys.tutorials.utils.spark.streaming;

import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.HadoopTutorialException;
import fr.finaxys.tutorials.utils.spark.utils.Converter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
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
    private static AtomConfiguration atom = AtomConfiguration.getInstance();
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaStreamingRequester.class);
    private static String kafkaTopic = atom.getKafkaTopic() ;
    private static String kafkaQuorum = atom.getKafkaQuorum() ;
    private static String hbaseConf = atom.getHbaseConfHbase();
    private static String tableName = atom.getTableName();
    private static byte[] columnFamily = atom.getColumnFamily();

    public static void main(String[] args){

        //create SparkConfig and try to start local or distributed mode
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

        //create kafka receiver
        Map<String, Integer> topics  = new HashMap<>();
        topics.put(kafkaTopic,new Integer(1));
        JavaPairReceiverInputDStream<String, String> kafkaStream = KafkaUtils.createStream(jssc,kafkaQuorum,"groupid",topics);

        // persist collected data
        kafkaStream.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {

            private static final long serialVersionUID = -6487126638165154032L;

            @Override
            public Void call(JavaPairRDD<String, String> rdd) throws Exception {
                // log data count
                LOGGER.info("data count = "+rdd.count());
                // create connection with HBase
                final Configuration config = new Configuration();
                try {
                    config.addResource(new Path(hbaseConf));
                    config.reloadConfiguration();
                    HBaseAdmin.checkHBaseAvailable(config);
                    LOGGER.info("HBase is running!");
                }
                catch (Exception e) {
                    LOGGER.error("HBase is not running!" + e.getMessage());
                    throw new HadoopTutorialException(e.getMessage());
                }
                config.set(TableInputFormat.INPUT_TABLE, tableName);
                config.set(TableInputFormat.INPUT_TABLE, tableName);

                final Job newAPIJobConfiguration1 ;
                try {
                    newAPIJobConfiguration1 = Job.getInstance(config);
                } catch (IOException e) {
                    LOGGER.error("can't create mapRed conf");
                    throw new HadoopTutorialException(e.getMessage());
                }
                newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
                newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);



                JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = rdd.mapToPair(new PairFunction<Tuple2<String, String>, ImmutableBytesWritable, Put>() {
                    @Override
                    public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                        Converter converter = new Converter(columnFamily);
                        Put put = converter.convertStringToPut(stringStringTuple2._1(),stringStringTuple2._2());
                        //LOGGER.info("data inserted : " + stringStringTuple2._1());
                        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
                    }
                });

                hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
                return null;
            }
        });

        //start spark streaming
        jssc.start();
        jssc.awaitTermination();
    }
}
