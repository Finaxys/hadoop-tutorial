package fr.finaxys.tutorials.utils.spark.utils;

import fr.finaxys.tutorials.utils.HadoopTutorialException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by finaxys on 12/9/15.
 */
public class RequestReader {

    private String filePath ;
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(RequestReader.class.getName());

    public String readRequest(){
        String request = null ;
        File file = new File(filePath); //for ex foo.txt
        FileReader reader = null;
        try {
            reader = new FileReader(file);
            char[] chars = new char[(int) file.length()];
            reader.read(chars);
            request = new String(chars);
            reader.close();
        } catch (IOException e) {
            LOGGER.severe("can't read request : "+e.getMessage());
            throw new HadoopTutorialException() ;
        } finally {
            if(reader !=null){
                try {
                    reader.close();
                } catch (IOException e) {
                    LOGGER.severe("Can't close reader :"+e.getMessage());
                }
            }
        }
        return request ;
    }

    public RequestReader(String filePath) {
        this.filePath = filePath;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

}
