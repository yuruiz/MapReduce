package example;

import java.util.ArrayList;
import java.util.List;

import task.ClientJob;
import util.KeyValuePair;

public class WordCount implements ClientJob {

	/**
	 * 
	 */
	private static final long serialVersionUID = 114150716109249973L;

	@Override
	public List<KeyValuePair> map(String key, String value) {

		List<KeyValuePair> list = new ArrayList<KeyValuePair>();

		String[] words = key.split(" ");
		for (String word : words) {
			KeyValuePair pair = new KeyValuePair(word, "1");
			list.add(pair);
		}

		return list;
	}

	@Override
	public KeyValuePair reduce(String key, ArrayList<String> values) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getfiles() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getMaxReduceFile() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getId() {
		// TODO Auto-generated method stub
		return 0;
	}

}
