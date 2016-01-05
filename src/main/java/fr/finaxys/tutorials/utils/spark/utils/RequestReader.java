package fr.finaxys.tutorials.utils.spark.utils;

import fr.finaxys.tutorials.utils.HadoopTutorialException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

/**
 * Created by finaxys on 12/9/15.
 */
public class RequestReader {

    private String filePath ;
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(RequestReader.class.getName());


    public RequestReader(String filePath) {
        this.filePath = filePath;
    }

    public String readRequest(){
        String request = null ;
        File file = new File(filePath); //for ex foo.txt
        FileReader reader = null;
        boolean tryHDFS = false;
        try {
            reader = new FileReader(file);
            char[] chars = new char[(int) file.length()];
            reader.read(chars);
            request = new String(chars);
            reader.close();
            LOGGER.info("request readed from "+filePath);
        } catch (IOException e) {
            LOGGER.info("Not able to load "+filePath+" from file. Trying to read "+filePath+" from hdfs. May be in MapReduce or Spark.");
            tryHDFS = true;
        }finally {
            if(reader !=null){
                try {
                    reader.close();
                } catch (IOException e) {
                    LOGGER.severe("Can't close reader :"+e.getMessage());
                }
            }
        }
        if (tryHDFS) {
            try {
                FileSystem fs = FileSystem.get(new Configuration()) ;
                Path pt=new Path(filePath);
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                StringBuffer buffer = new StringBuffer();
                line=br.readLine();
                while (line != null){
                    buffer.append(line + "\n");
                    line=br.readLine();
                }
                br.close();
                request =  buffer.toString() ;
                LOGGER.info("request readed from hdfs : " + filePath);
            } catch (IOException ioe) {
                LOGGER.info("Not able to load "+filePath+" from HDFS. ");
                throw new HadoopTutorialException("could not load  "+filePath+" on file system or hdfs.");
            }
        }

        return request ;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

}
