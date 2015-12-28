package fr.finaxys.tutorials.utils.spark.streaming;

import fr.finaxys.tutorials.utils.AtomConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by finaxys on 12/22/15.
 */
public class KafkaStreamingRequester {
    private static JavaSparkContext jsc;
    private static AtomConfiguration atom = new AtomConfiguration();
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaStreamingRequester.class);

    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("sparkApplication");
        try{
            jsc = new JavaSparkContext(sparkConf);
            LOGGER.info("spark started with distributed mode");
        }catch(Exception e){
            LOGGER.info("spark can't start with distributed mode");
            sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkApplication");
            jsc = new JavaSparkContext(sparkConf);
        }
        JavaStreamingContext jssc = new JavaStreamingContext(jsc,
                Durations.seconds(2));
        Map<String, Integer> topics  = new HashMap<>();
        topics.put(atom.getKafkaTopic(),new Integer(1));
        JavaPairReceiverInputDStream<String, String> kafkaStream = KafkaUtils.createStream(jssc,"localhost:2181","groupid",topics);
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

        jssc.start();
        jssc.awaitTermination();
    }
}
