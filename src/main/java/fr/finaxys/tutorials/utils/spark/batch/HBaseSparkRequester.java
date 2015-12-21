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
import scala.Tuple2;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by finaxys on 12/8/15.
 */
public class HBaseSparkRequester implements Serializable {

    public static  AtomConfiguration atomConfiguration;
    public static  String hbaseSitePath ;
    public static final RequestReader requestReader= new RequestReader("spark-requests/hbase-batch-request.sql");
    public static Configuration hbaseConf = null ;
    public static int max = 10000 ;
    public static String resultQualifier = "result" ;

    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(HBaseSparkRequester.class.getName());


    public HBaseSparkRequester(AtomConfiguration atomConfiguration) {
        this.atomConfiguration = atomConfiguration ;
        hbaseSitePath = atomConfiguration.getHbaseConfHbase() ;
    }

    public HBaseSparkRequester(Configuration hbaseConf) {
        this.hbaseConf = hbaseConf ;
        atomConfiguration = new AtomConfiguration();
        hbaseSitePath = atomConfiguration.getHbaseConfHbase() ;
    }

    public HBaseSparkRequester() {
        atomConfiguration = new AtomConfiguration();
        hbaseSitePath = atomConfiguration.getHbaseConfHbase() ;
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
        String tableName = atomConfiguration.getTableName();
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        //conf.reloadConfiguration();
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
        LOGGER.info("Number of Records found : " + mapped.count());
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.createDataFrame(mapped, DataRow.class);
        df.registerTempTable("records");
        DataFrame df2 = sqlContext.sql(request);
        Row[] rows = df2.collect();
        df2.show();

        // put data
        HBaseAnalysis analysis = new HBaseAnalysis();
        analysis.setTableName(TableName.valueOf(atomConfiguration.getSparkTableName()));
        analysis.setColumnFamily(atomConfiguration.getColumnFamily());
        analysis.setHbaseConfiguration(conf);
        analysis.openTable();
        Date date = new Date();
        Put p = new Put(Bytes.toBytes("r-"+date.getTime()));
        p.addColumn(atomConfiguration.getColumnFamily(),Bytes.toBytes(resultQualifier),Bytes.toBytes(df2.showString(max,false)));
        analysis.directPutTable(p);
        analysis.closeTable();


        sc.stop();
        return rows ;
    }


    public static void main(String[] args) {
            HBaseSparkRequester analysis = new HBaseSparkRequester() ;
            analysis.executeRequest();
    }
}
