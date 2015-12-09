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

/**
 * Created by finaxys on 12/8/15.
 */
public class HBaseAnalysis {

    public static final AtomConfiguration atomConfiguration = new AtomConfiguration() ;
    public static final String hbaseSitePath = atomConfiguration.getHbaseConfHbase() ;
    public static final RequestReader requestReader= new RequestReader("spark-requests/hbase-analysis.sql");

    public static void main(String[] args) {
        String request = requestReader.readRequest() ;
        SparkConf sparkConf = new SparkConf().setAppName("HBaseAnalysis")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Configuration conf = HBaseConfiguration.create();
        String tableName = "trace";
        conf.addResource(new Path(hbaseSitePath));
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        conf.reloadConfiguration();
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc
                .newAPIHadoopRDD(conf, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);
        JavaRDD<DataRow> mapped = hBaseRDD
                .map(new Function<Tuple2<ImmutableBytesWritable, Result>, DataRow>() {

                    private static final long serialVersionUID = -570690400768209214L;

                    @Override
                    public DataRow call(Tuple2<ImmutableBytesWritable, Result> tuple)
                            throws Exception {
                        Converter converter = new Converter();
                        DataRow dr = converter.convertTupleToDataRow(tuple);
                        return dr;
                    }
                });
        System.out.println("Number of Records found : " + mapped.count());
        System.out.println("First records found : " + mapped.first());
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.createDataFrame(mapped, DataRow.class);
        df.registerTempTable("records");
        DataFrame df2 = sqlContext.sql(request);
        df2.show();
        sc.stop();
    }
}
