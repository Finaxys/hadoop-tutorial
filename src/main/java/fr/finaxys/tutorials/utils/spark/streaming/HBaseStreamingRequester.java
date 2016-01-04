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

import com.sun.istack.NotNull;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

public class HBaseStreamingRequester implements Serializable{
	
   private static final String RESULT_QUALIFIER = "result";
   private static final int MAX = 10000 ;

   static private JavaSparkContext jsc;
   private Long lastTS;
   static  private Configuration conf;
   private JavaRDD<DataRow> mainRDDs ;
   
   private String hbaseSitePath ;
   private String tableName ;
   private String sparkTableName;
   private byte[] columnFamily;
   private String request;
   static private JavaStreamingContext jssc;
   
   public HBaseStreamingRequester(@NotNull AtomConfiguration atomConfiguration) {
       hbaseSitePath = atomConfiguration.getHbaseConfHbase();
       tableName = atomConfiguration.getTableName();
       sparkTableName = atomConfiguration.getSparkTableName();
       columnFamily = atomConfiguration.getColumnFamily();
       request = new RequestReader("spark-requests/hbase-streaming-request.sql").readRequest();
   }
   
   public void initSparkStreamContext() {
       SparkConf sparkConf = new SparkConf().setAppName("sparkApplication");
       try{
           jsc = new JavaSparkContext(sparkConf);
       } catch (Exception e){
       	// if we don't find a distributed spark context, start it in local
           sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkApplication");
           jsc = new JavaSparkContext(sparkConf);
       }
       jssc = new JavaStreamingContext(jsc, Durations.seconds(2));
   }
   
   public void initStream() {
	   
       JavaReceiverInputDStream<DataRow> jrids = jssc
               .receiverStream(new EmptyReceiver(StorageLevel.MEMORY_ONLY()));
       final Converter converter = new Converter(columnFamily);
       conf = HBaseConfiguration.create();
       conf.addResource(new Path(hbaseSitePath));
       conf.set(TableInputFormat.INPUT_TABLE, tableName);

       //
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
               df2.show(MAX);

               // put data
               HBaseAnalysis analysis = new HBaseAnalysis();
               analysis.setTableName(TableName.valueOf(sparkTableName));
               analysis.setColumnFamily(columnFamily);
               analysis.setHbaseConfiguration(conf);
               analysis.openTable();
               Date date = new Date();
               Put p = new Put(Bytes.toBytes("r-"+date.getTime()));
               p.addColumn(columnFamily,Bytes.toBytes(RESULT_QUALIFIER),Bytes.toBytes(df2.showString(MAX,false)));
               analysis.directPutTable(p);
               analysis.closeTable();

               return null;
           }
       });
   }
   
   public void startStream() {
	   jssc.start();
       jssc.awaitTermination();
   }
   
   public void stopStream() {
	   jssc.close();
   }
     
    /**
     * @param args
     */
    public static void main(String[] args) {
    	AtomConfiguration conf = AtomConfiguration.getInstance();
    	HBaseStreamingRequester requester = new HBaseStreamingRequester(conf);
    	requester.initSparkStreamContext();
    	requester.initStream();
    	requester.startStream();
    	requester.stopStream();
    }

}
