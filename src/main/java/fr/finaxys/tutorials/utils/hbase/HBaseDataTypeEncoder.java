package fr.finaxys.tutorials.utils.hbase;

import org.apache.hadoop.hbase.types.DataType;
import org.apache.hadoop.hbase.types.OrderedBlobVar;
import org.apache.hadoop.hbase.types.RawByte;
import org.apache.hadoop.hbase.types.RawDouble;
import org.apache.hadoop.hbase.types.RawInteger;
import org.apache.hadoop.hbase.types.RawLong;
import org.apache.hadoop.hbase.types.RawStringTerminated;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;

import com.sun.istack.NotNull;

public class HBaseDataTypeEncoder
{

    private final DataType<String> strDataType = new RawStringTerminated("\0");
    private final DataType<Integer> intDataType = new RawInteger();
    private final DataType<Long> longDataType = new RawLong();
    private final DataType<Double> doubleDataType = new RawDouble();
    private final DataType<byte[]> charType = OrderedBlobVar.ASCENDING;
    private final DataType<Byte> boolDataType = new RawByte();

    @NotNull
    public byte[] encodeString(@NotNull String value)
    {
        return encode(strDataType, value);
    }

    @NotNull
    public byte[] encodeInt(int value)
    {
        return encode(intDataType, value);
    }

    @NotNull
    public byte[] encodeBoolean(boolean value)
    {
        return encode(boolDataType, (byte) (value ? 1 : 0));
    }

    @NotNull
    public byte[] encodeLong(long value)
    {
        return encode(longDataType, value);
    }

    @NotNull
    public byte[] encodeDouble(@NotNull Number value)
    {
        return encode(doubleDataType, value.doubleValue());

    }

    @NotNull
    public byte[] encodeChar(@NotNull char value)
    {
        return encode(charType, charToBytes(value));
    }

    /**
     * Return an array of 2 bytes
     *
     * @param c ascii or not (example : 'é', '^o', 'ç'...)
     * @return encoded char as byte array
     */
    @NotNull
    private static byte[] charToBytes(@NotNull Character c)
    {
        byte[] b = new byte[2];
        b[0] = (byte) ((c & 0xFF00) >> 8);
        b[1] = (byte) (c & 0x00FF);
        return b;
    }

    @NotNull
    private <T> byte[] encode(@NotNull DataType<T> dt, @NotNull T value)
    {
    	SimplePositionedMutableByteRange sbpr = new SimplePositionedMutableByteRange(dt.encodedLength(value));
        dt.encode(sbpr, value);
        return sbpr.getBytes();
    }


    @NotNull
    public String decodeString(@NotNull byte[] value)
    {
        String stringValue = Bytes.toString(value) ;
        return stringValue.substring(0,stringValue.length()-1) ;

    }

    @NotNull
    public int decodeInt(@NotNull byte[] value)
    {
        return Bytes.toInt(value);
    }

    @NotNull
    public boolean decodeBoolean(@NotNull byte[] value)
    {
        return Bytes.toBoolean(value);
    }

    @NotNull
    public long decodeLong(@NotNull byte[] value)
    {
        return Bytes.toLong(value);
    }

    @NotNull
    public Number decodeDouble( @NotNull byte[] value)
    {
        return Bytes.toDouble(value);

    }

}