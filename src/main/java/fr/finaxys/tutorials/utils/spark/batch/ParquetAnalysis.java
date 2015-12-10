package fr.finaxys.tutorials.utils.spark.batch;

import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.spark.utils.RequestReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by finaxys on 12/8/15.
 */
public class ParquetAnalysis {

    public static final AtomConfiguration atomConfiguration = new AtomConfiguration() ;
    public static final String hdfsSitePAth = atomConfiguration.getHadoopConfHdfs() ;
    public static final RequestReader requestReader= new RequestReader("spark-requests/parquet-analysis.sql");

    public static void main(String[] args)  {
        String request = requestReader.readRequest();
        SparkConf sparkConf = new SparkConf().setAppName("ParquetAnalysis");
        JavaSparkContext sc = null ;
        try{
            sc = new JavaSparkContext(sparkConf);
        }catch(Exception e){
            sparkConf = new SparkConf().setAppName("ParquetAnalysis")
                    .setMaster("local[*]");
            sc = new JavaSparkContext(sparkConf);
        }
        Configuration conf = new Configuration();
        conf.addResource(new Path(hdfsSitePAth));
        conf.reloadConfiguration();
        System.out.println("fs.default.name : - " + conf.get("fs.default.name"));
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().load(conf.get("fs.default.name")+"/"+atomConfiguration.getParquetHDFSDest());
        df.registerTempTable("records");
        DataFrame df2 = sqlContext.sql(request);
        df2.show(1000);
        sc.stop();
    }
}
