package fr.finaxys.tutorials.utils.hdfs;

import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.HadoopTutorialException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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

    public HDFSReader(AtomConfiguration atomConf){
        this.conf = new Configuration();
        this.conf.addResource(new Path(atomConf.getHadoopConfHdfs()));
        this.conf.reloadConfiguration();
        try {
            this.fs = FileSystem.get(this.conf);
        } catch (IOException e) {
            throw new HadoopTutorialException("hdfs-site.xml can't be loade",e) ;
        }

    }

    public HDFSReader(Configuration conf){
        this.conf = conf;
        conf.reloadConfiguration();
        try {
            this.fs = FileSystem.get(this.conf);
        } catch (IOException e) {
            throw new HadoopTutorialException("hdfs-site.xml can't be loade",e) ;
        }

    }

    public void setConf(Configuration conf){
        this.conf = conf ;
    }

    public String getHDFSFile (String filePath,int max) throws Exception{
        Path pt=new Path(filePath);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        StringBuffer buffer = new StringBuffer();
        line=br.readLine();
        int i = 0 ;
        while (line != null && i<max ){
            buffer.append(line + "\n");
            line=br.readLine();
            i++ ;
        }
        br.close();
        return buffer.toString() ;
    }

    public void lsDirectory(String dirPath) throws IOException {
        FileStatus[] fst = fs.listStatus(new Path(dirPath)) ;
        for(FileStatus f : fst){
            System.out.println(f.toString());
        }
    }

    public void close() throws IOException {
        fs.close();
    }

    public static void main(String[] args) throws Exception {
        HDFSReader reader = new HDFSReader(new AtomConfiguration());
        System.out.println(reader.getHDFSFile("/finalResult/part-m-00000", 10));
        reader.close();
    }
}
