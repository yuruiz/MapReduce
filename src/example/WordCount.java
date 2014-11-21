package example;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import task.ClientJob;
import util.KeyValuePair;

/**
 * A example that conforms to the job interface which implements basic word
 * count
 * 
 * @author siyuwei
 *
 */
public class WordCount implements ClientJob {

	/**
	 * 
	 */
	private static final long serialVersionUID = 114150716109249973L;

	/**
	 * Iterate over the words, for each word, emit "word 1"
	 */
	@Override
	public List<KeyValuePair> map(String key, String value) {

		List<KeyValuePair> list = new ArrayList<KeyValuePair>();

		String[] words = value.split("\\W+");
		for (String word : words) {
			word = word.toLowerCase();
			KeyValuePair pair = new KeyValuePair(word, "1");
			list.add(pair);
		}
		/*
		 * Slow down this a little bit so we can shut it down
		 */
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * Count the number of appearance of a word
	 */
	@Override
	public KeyValuePair reduce(String key, List<String> values) {
		int sum = 0;

		for (String s : values) {
			sum += Integer.parseInt(s);
		}
		/*
		 * Slow down this a little bit so we can shut it down
		 */
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return new KeyValuePair(key, String.valueOf(sum));
	}

	@Override
	public List<String> getfiles() {
		List<String> files = new ArrayList<String>();
		try {
			Scanner s = new Scanner(new FileInputStream("data.txt"));
			while (s.hasNextLine()) {
				String file = s.nextLine();
				files.add(file);
			}
			s.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return files;
	}

	@Override
	public int getMaxReduceFile() {
		return 3;
	}

}
