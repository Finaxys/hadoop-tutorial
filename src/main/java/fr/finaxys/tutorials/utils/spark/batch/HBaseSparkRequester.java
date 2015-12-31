package fr.finaxys.tutorials.utils.spark.batch;

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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.sun.istack.NotNull;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Date;

public class HBaseSparkRequester implements Serializable {

	private static final long serialVersionUID = -1412858030538057531L;
	//private AtomConfiguration atomConfiguration;
    private String hbaseSitePath;
    transient private final RequestReader requestReader = new RequestReader("spark-requests/hbase-batch-request.sql");
    transient private Configuration hbaseConf = null ;
    private static final int MAX = 10000 ;
    // @TODO made it a parameter
    private final static byte[] RESULT_QUALIFIER = Bytes.toBytes("result");
    private String tableName;
    private byte[] columnFamily;
    private String sparkTableName;

    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(HBaseSparkRequester.class.getName());

    public HBaseSparkRequester(@NotNull String hbaseSitePath, @NotNull String tableName,
    		@NotNull byte[] columnFamily, @NotNull String sparkTableName) {
        this.hbaseSitePath = hbaseSitePath;
        this.tableName = tableName;
        this.sparkTableName = sparkTableName;
        this.columnFamily = columnFamily;
    }

    public HBaseSparkRequester(Configuration hbaseConf, @NotNull String tableName,
    		@NotNull byte[] columnFamily, @NotNull String sparkTableName) {
        this.hbaseConf = hbaseConf ;
        this.tableName = tableName;
        this.sparkTableName = sparkTableName;
        this.columnFamily = columnFamily;
    }

    public Row[] executeRequest(){
        return executeRequest(requestReader.readRequest());
    }

    public Row[] executeRequest(String request){
        SparkConf sparkConf = new SparkConf().setAppName("HBaseAnalysis");
        JavaSparkContext sc = null ;
        //try to launch spark with distributed mode then with the local one
        try{
            sc = new JavaSparkContext(sparkConf);
        }catch(Exception e){
            LOGGER.info("Disable distributed mode and try the local one .");
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
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        //conf.reloadConfiguration();
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
        LOGGER.info("Number of Records found : " + mapped.count());
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.createDataFrame(mapped, DataRow.class);
        df.registerTempTable("records");
        DataFrame df2 = sqlContext.sql(request);
        Row[] rows = df2.collect();
        df2.show();

        // put data
        HBaseAnalysis analysis = new HBaseAnalysis();
        analysis.setTableName(TableName.valueOf(sparkTableName));
        analysis.setColumnFamily(columnFamily);
        analysis.setHbaseConfiguration(conf);
        analysis.openTable();
        Date date = new Date();
        Put p = new Put(Bytes.toBytes("r-"+date.getTime()));
        p.addColumn(columnFamily,RESULT_QUALIFIER,Bytes.toBytes(df2.showString(MAX,false)));
        analysis.directPutTable(p);
        analysis.closeTable();


        sc.stop();
        return rows ;
    }
    
    public static void main(String[] args) {
    	AtomConfiguration atomConfiguration = AtomConfiguration.getInstance();
        HBaseSparkRequester analysis = new HBaseSparkRequester(
        		atomConfiguration.getHbaseConfHbase(),
        		atomConfiguration.getTableName(), 
        		atomConfiguration.getColumnFamily(),
        		atomConfiguration.getSparkTableName()
        		);
        analysis.executeRequest();
    }
}
