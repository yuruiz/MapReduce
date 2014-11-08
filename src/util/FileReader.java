package util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by yuruiz on 11/7/14.
 */
public class FileReader {

    private String fileName;


    public FileReader(String filename) {
        this.fileName = filename;
    }

    public String[][] getKeyValuePairs(int index, int len) {
        String[][] keyValuePairs = null;
        try {
            RandomAccessFile input = new RandomAccessFile(fileName, "r");
            int recordcount = 0;

            while (recordcount < index) {
                input.readLine();
                recordcount++;
            }

            keyValuePairs = new String[len][2];

            for (int i = 0; i < len; i++) {
                String linebuf = input.readLine();
                if (linebuf == null) {
                    break;
                }

                keyValuePairs[i][0] = ((Integer)(recordcount + i)).toString();
                keyValuePairs[i][1] = linebuf;
            }

            input.close();


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return keyValuePairs;
    }
}
