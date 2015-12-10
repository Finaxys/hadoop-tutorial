package fr.finaxys.tutorials.utils.spark.utils;

import fr.finaxys.tutorials.utils.hbase.HBaseDataTypeEncoder;
import fr.finaxys.tutorials.utils.spark.models.DataRow;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by finaxys on 12/4/15.
 */
public class Converter implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 3142828016264704546L;
    private transient Map<byte[], byte[]> cfmap;
    private static transient HBaseDataTypeEncoder encoder = new HBaseDataTypeEncoder() ;

    public Converter() {
    }

    public DataRow convertTupleToDataRow(
            Tuple2<ImmutableBytesWritable, Result> tuple) {
        cfmap = tuple._2.getFamilyMap(Bytes.toBytes("cf"));
        DataRow dr = new DataRow() {
            {
                setTrace(getString("Trace"));
                setNumDay(getInteger("NumDay"));
                setNumTick(getInteger("NumTick"));
                setBestBid(getLong("BestBid"));
                setBestAsk(getLong("BestAsk"));
                setOrder2(getString("Order2"));
                setOrder1(getString("Order1"));
                setDir(getChar("Dir"));
                setObName(getString("ObName"));
                setValidity(getLong("Validity"));
                setQuantity(getInteger("Quantity"));
                setId(getLong("Id"));
                setType(getChar("Type"));
                setExtId(getString("ExtId"));
                setSender(getString("Sender"));
                setNbPricesFixed(getLong("NbPricesFixed"));
                setLastFixedPrice(getLong("LastFixedPrice"));
                setHighestPrice(getLong("HighestPrice"));
                setLowestPrice(getLong("LowestPrice"));
                setFirstFixedPrice(getLong("FirstFixedPrice"));
                setOrderExtId(getString("OrderExtId"));
                setTimestamp(getLong("Timestamp"));
                setDirection(getChar("Direction"));
                setPrice(getLong("Price"));
                setExecuted(getInteger("Executed"));
                setCash(getLong("Cash"));
                setAgentRefId(getInteger("AgentRefId"));
                setAgentName(getString("AgentName"));
                setIsMarketMaker(getBoolean("isMarketMaker"));
                setDetails(getString("Details")) ;
            }
        };
        return dr;
    }

    private String getString(String col) {
        try {
            byte[] bArr = cfmap.get(Bytes.toBytes(col));
            return encoder.decodeString(bArr);
        }
        catch (Exception e){
            return null ;
        }
    }

    private Number getDouble(String col) {
        try {
            byte[] bArr = cfmap.get(Bytes.toBytes(col));
            return (bArr != null) ? encoder.decodeDouble(bArr) : null;
        }
        catch(Exception e){
            return 0 ;
        }
    }

    private Integer getInteger(String col) {
        try{
            byte[] bArr = cfmap.get(Bytes.toBytes(col));
            return (bArr != null) ? encoder.decodeInt(bArr): null;
        }
        catch(Exception e){
            return null ;
        }
    }

    private Long getLong(String col) {
        try {
            byte[] bArr = cfmap.get(Bytes.toBytes(col));
            return (bArr != null) ? encoder.decodeLong(bArr) : null;
        }
        catch (Exception e){
            return null ;
        }
    }

    private String getChar(String col) {
        try {
            byte[] bArr = cfmap.get(Bytes.toBytes(col));
            return (bArr != null) ? encoder.decodeChar(bArr)+"": null;
        }
        catch(Exception e){
            return null ;
        }
    }

    private Boolean getBoolean(String col) {
        try{
            byte[] bArr = cfmap.get(Bytes.toBytes(col));
            return (bArr != null) ? encoder.decodeBoolean(bArr): null;
        }
        catch (Exception e){
            return null ;
        }
    }
}
