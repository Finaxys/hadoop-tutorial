package fr.finaxys.tutorials.utils.spark.batch;

import fr.finaxys.tutorials.utils.AtomConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;

/**
 * Created by finaxys on 12/8/15.
 */
public class ParquetAnalysis {

    public static final AtomConfiguration atomConfiguration = new AtomConfiguration() ;
    public static final String hdfsSitePAth = atomConfiguration.getHadoopConfHdfs() ;

    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf().setAppName("HBaseRead")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Configuration conf = new Configuration();
        conf.addResource(new Path(hdfsSitePAth));
        conf.reloadConfiguration();
        System.out.println("fs.default.name : - " + conf.get("fs.default.name"));
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().load(conf.get("fs.default.name")+"/"+atomConfiguration.getParquetHDFSDest());
        df.registerTempTable("records");
        DataFrame df2 = sqlContext.sql("SELECT order.ObName FROM records where type='Order'");
        df2.show(1000);
        sc.stop();
    }
}
