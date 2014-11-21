package task;

import util.KeyValuePair;

import java.io.Serializable;
import java.util.List;

public interface MapReduceMethod extends Serializable {
	/**
	 * The map operation defined for this map/reduce job
	 * 
	 * @param key
	 *            the key
	 * @param value
	 *            the value
	 * @return the list of key pair values created
	 */
	public List<KeyValuePair> map(String key, String value);

	/**
	 * Reduce a key and a list of value into a single key pair value
	 * 
	 * @param key
	 *            the key
	 * @param value
	 *            list of values
	 * @return reduced key pair value
	 */
	public KeyValuePair reduce(String key, List<String> values);

}
