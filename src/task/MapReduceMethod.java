package task;

import util.KeyValuePair;

import java.io.Serializable;
import java.util.List;

public interface MapReduceMethod extends Serializable {
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
