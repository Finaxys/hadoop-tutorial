package fr.finaxys.tutorials.utils.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;


/**
 * Created by finaxys on 11/20/15.
 */
public class HDFSReader {
    public static void main (String [] args) throws Exception{
        try{
            Path pt=new Path("/priceParquetp");
            Configuration conf = new Configuration();
            conf.addResource(new Path("/tmp/configuration.xml"));
            conf.reloadConfiguration();
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fst = fs.listStatus(pt);
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line=br.readLine();
            while (line != null){
                System.out.println(line);
                line=br.readLine();
            }
        }catch(Exception e){
        }
    }
}
