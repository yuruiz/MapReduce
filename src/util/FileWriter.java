package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * This class writes intermediate result to local disk
 * 
 * Created by yuruiz on 11/7/14.
 */
public class FileWriter {
	protected String fileName;

	public FileWriter(String fileName) {
		this.fileName = fileName;
	}

	/**
	 * Take a list of key value pairs and write it to the disk
	 * 
	 * @param recordlist
	 */
	public void write(List<KeyValuePair> recordlist) {
		try {
			FileOutputStream outputStream = new FileOutputStream(new File(
					fileName), false);

			for (KeyValuePair tempPair : recordlist) {
				String linebuf = tempPair.getKey() + "\t" + tempPair.getValue()
						+ "\n";
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
