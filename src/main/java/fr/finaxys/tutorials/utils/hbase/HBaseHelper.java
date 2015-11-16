package fr.finaxys.tutorials.utils.hbase;

/**
 * Created by finaxys on 11/16/15.
 */
public class HBaseHelper {

    static public String decodeStringRowKey(String rowKey){
        return rowKey.replaceAll("\u0000","");
    }

}
