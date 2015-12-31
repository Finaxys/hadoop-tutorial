package fr.finaxys.tutorials.utils.spark.batch;

import java.io.Serializable;

import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.spark.utils.RequestReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.sun.istack.NotNull;

public class ParquetSparkRequester implements Serializable {

	private static final long serialVersionUID = 3422217377336638635L;
	//private AtomConfiguration atomConfiguration;
    private String hdfsSitePath ;
    private Configuration hdfsConf ;
    private String parquetHDFSDest;
    transient private final RequestReader requestReader= new RequestReader("spark-requests/parquet-batch-request.sql");

    public ParquetSparkRequester(@NotNull String hadoopConfHdfs, 
    		@NotNull String parquetHDFSDest) {
        this.hdfsSitePath = hadoopConfHdfs;
        this.parquetHDFSDest = parquetHDFSDest;
    }

    public ParquetSparkRequester(Configuration hdfsConf, @NotNull String parquetHDFSDest) {
        this.hdfsConf = hdfsConf ;
        this.parquetHDFSDest = parquetHDFSDest;
    }


    public Row[] executeRequest(){
        return executeRequest(requestReader.readRequest());
    }

    public Row[] executeRequest(String request){
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
            conf.addResource(new Path(hdfsSitePath));
        } else {
            conf = this.hdfsConf ;
        }
        conf.reloadConfiguration();
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().load(conf.get("fs.default.name")+"/"+parquetHDFSDest);
        df.registerTempTable("records");
        DataFrame df2 = sqlContext.sql(request);
        df2.show(1000);
        Row[] result = df2.collect();
        sc.stop();
        return result ;
    }

    public static void main(String[] args)  {
    	AtomConfiguration conf = AtomConfiguration.getInstance();
        ParquetSparkRequester analysis = new ParquetSparkRequester(conf.getHadoopConfHdfs(), conf.getParquetHDFSDest()) ;
        analysis.executeRequest();
    }
}
