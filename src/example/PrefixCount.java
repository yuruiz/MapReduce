package example;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import task.ClientJob;
import util.Config;
import util.KeyValuePair;

public class PrefixCount implements ClientJob {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1047693342340616319L;

	/**
	 * Iterate over the words in a record which is a line, for each prefix
	 */
	@Override
	public List<KeyValuePair> map(String key, String value) {
		List<KeyValuePair> list = new ArrayList<KeyValuePair>();

		/*
		 * Split the line into words
		 */
		String[] words = value.split("\\W+");
		for (String word : words) {
			word = word.toLowerCase();
			/*
			 * For all the prefix of this word, write down "prefix, word"
			 */
			for (int i = 1; i < word.length(); i++) {
				String prefix = word.substring(0, i);
				list.add(new KeyValuePair(prefix, word));
			}

		}
		return list;
	}

	@Override
	public KeyValuePair reduce(String key, List<String> values) {
		Map<String, Integer> map = new HashMap<String, Integer>();
		String result = "";
		int max = 0;

		/*
		 * For every prefix, calculate the word that appears the most for this
		 * given prefix
		 */
		for (String s : values) {
			if (!map.containsKey(s)) {
				map.put(s, 1);
			} else {
				map.put(s, map.get(s) + 1);
			}
			if (map.get(s) > max) {
				max = map.get(s);
				result = s;
			}

		}

		/*
		 * write prefix and the word has this prefix and appears the most
		 */
		return new KeyValuePair(key, result);
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
			e.printStackTrace();
		}

		return files;
	}

	@Override
	public int getMaxReduceFile() {
		return Config.MAX_REDUCE;
	}

}
