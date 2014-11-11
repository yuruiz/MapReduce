package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Created by yuruiz on 11/7/14.
 */
public class FileWriter {
    String filename;

    public FileWriter(String fileName){
        filename = fileName;
    }

    public void write(List<KeyValuePair> recordlist) {
        try {
            FileOutputStream outputStream = new FileOutputStream(new File(filename), false);

            for (KeyValuePair tempPair: recordlist) {
                String linebuf = tempPair.getKey() + "\t" + tempPair.getValue() + "\n";
                outputStream.write(linebuf.getBytes());
            }

            outputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
