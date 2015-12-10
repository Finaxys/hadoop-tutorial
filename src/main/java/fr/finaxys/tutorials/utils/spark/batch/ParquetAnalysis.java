package fr.finaxys.tutorials.utils.spark.batch;

import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.spark.utils.RequestReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

/**
 * Created by finaxys on 12/8/15.
 */
public class ParquetAnalysis implements Serializable{

    public static  AtomConfiguration atomConfiguration;
    public static  String hdfsSitePAth ;
    public static  Configuration hdfsConf ;
    public static final RequestReader requestReader= new RequestReader("spark-requests/parquet-analysis.sql");

    public ParquetAnalysis(AtomConfiguration atomConfiguration) {
        this.atomConfiguration = atomConfiguration ;
        this.hdfsSitePAth = atomConfiguration.getHadoopConfHdfs();
    }

    public ParquetAnalysis(Configuration hdfsConf) {
        this.hdfsConf = hdfsConf ;
        this.atomConfiguration = new AtomConfiguration() ;
        this.hdfsSitePAth = atomConfiguration.getHadoopConfHdfs();
    }

    public ParquetAnalysis() {
        this.atomConfiguration = new AtomConfiguration() ;
        this.hdfsSitePAth = atomConfiguration.getHadoopConfHdfs();
    }

    public DataFrame executeRequest(){
        return executeRequest(requestReader.readRequest());
    }

    public DataFrame executeRequest(String request){
        SparkConf sparkConf = new SparkConf().setAppName("ParquetAnalysis");
        JavaSparkContext sc = null ;
        try{
            sc = new JavaSparkContext(sparkConf);
        }catch(Exception e){
            sparkConf = new SparkConf().setAppName("ParquetAnalysis")
                    .setMaster("local[*]");
            sc = new JavaSparkContext(sparkConf);
        }
        Configuration conf ;
        if(hdfsConf == null){
            conf = new Configuration();
            conf.addResource(new Path(hdfsSitePAth));
        }
        else{
            conf = this.hdfsConf ;
        }
        conf.reloadConfiguration();
        System.out.println("fs.default.name : - " + conf.get("fs.default.name"));
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().load(conf.get("fs.default.name")+"/"+atomConfiguration.getParquetHDFSDest());
        df.registerTempTable("records");
        DataFrame df2 = sqlContext.sql(request);
        df2.show(1000);
        sc.stop();
        return df2 ;
    }

    public static void main(String[] args)  {
        ParquetAnalysis analysis = new ParquetAnalysis() ;
        analysis.executeRequest();
    }
}
