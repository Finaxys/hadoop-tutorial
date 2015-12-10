package fr.finaxys.tutorials.utils.spark.batch;

import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.spark.models.DataRow;
import fr.finaxys.tutorials.utils.spark.utils.Converter;
import fr.finaxys.tutorials.utils.spark.utils.RequestReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by finaxys on 12/8/15.
 */
public class HBaseAnalysis implements Serializable {

    public static  AtomConfiguration atomConfiguration;
    public static  String hbaseSitePath ;
    public static final RequestReader requestReader= new RequestReader("spark-requests/hbase-analysis.sql");
    public static Configuration hbaseConf = null ;

    public HBaseAnalysis(AtomConfiguration atomConfiguration) {
        this.atomConfiguration = atomConfiguration ;
        hbaseSitePath = atomConfiguration.getHbaseConfHbase() ;
    }

    public HBaseAnalysis(Configuration hbaseConf) {
        this.hbaseConf = hbaseConf ;
        atomConfiguration = new AtomConfiguration();
        hbaseSitePath = atomConfiguration.getHbaseConfHbase() ;
    }

    public HBaseAnalysis() {
        atomConfiguration = new AtomConfiguration();
        hbaseSitePath = atomConfiguration.getHbaseConfHbase() ;
    }

    public DataFrame executeRequest(){
        return executeRequest(requestReader.readRequest());
    }

    public DataFrame executeRequest(String request){
        SparkConf sparkConf = new SparkConf().setAppName("HBaseAnalysis");
        JavaSparkContext sc = null ;
        try{
            sc = new JavaSparkContext(sparkConf);
        }catch(Exception e){
            sparkConf = new SparkConf().setAppName("HBaseAnalysis")
                    .setMaster("local[*]");
            sc = new JavaSparkContext(sparkConf);
        }
        Configuration conf ;
        if(hbaseConf == null){
            conf = HBaseConfiguration.create();
            conf.addResource(new Path(hbaseSitePath));
        }
        else{
            conf = this.hbaseConf ;
        }
        String tableName = atomConfiguration.getTableName();
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        conf.reloadConfiguration();
        final byte[] columnFamily = atomConfiguration.getColumnFamily();
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc
                .newAPIHadoopRDD(conf, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);
        JavaRDD<DataRow> mapped = hBaseRDD
                .map(new Function<Tuple2<ImmutableBytesWritable, Result>, DataRow>() {

                    private static final long serialVersionUID = -570690400768209214L;

                    @Override
                    public DataRow call(Tuple2<ImmutableBytesWritable, Result> tuple)
                            throws Exception {
                        Converter converter = new Converter(columnFamily);
                        DataRow dr = converter.convertTupleToDataRow(tuple);
                        return dr;
                    }
                });
        System.out.println("Number of Records found : " + mapped.count());
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.createDataFrame(mapped, DataRow.class);
        df.registerTempTable("records");
        DataFrame df2 = sqlContext.sql(request);
        df2.show();
        sc.stop();
        return df2 ;
    }

    public static void main(String[] args) {
            HBaseAnalysis analysis = new HBaseAnalysis() ;
            analysis.executeRequest();
    }
}
