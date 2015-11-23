package fr.finaxys.tutorials.utils.hdfs;

import fr.finaxys.tutorials.utils.AtomConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * Created by finaxys on 11/20/15.
 */
public class HDFSReader {

    private Configuration conf ;
    private FileSystem fs ;

    public void HDFSReader(AtomConfiguration atomConf){
        this.conf = new Configuration();
        this.conf.addResource(new Path(atomConf.getHadoopConfHdfs()));
        this.conf.reloadConfiguration();
        try {
            this.fs = FileSystem.get(this.conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public  void showHDFSFile (String filePath) throws Exception{
        try{
            Path pt=new Path(filePath);
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
