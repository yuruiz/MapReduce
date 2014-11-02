package task;

import java.io.Serializable;
import java.util.List;

import util.KeyValuePair;

public interface MapReduceJob extends Serializable {
	/**
	 * The map operation defined for this map/reduce job
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public List<KeyValuePair> map(String key, String value);

	public KeyValuePair reduce(String key, String value);

}
