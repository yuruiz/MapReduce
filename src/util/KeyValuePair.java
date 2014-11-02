package util;

import java.io.Serializable;

public class KeyValuePair implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5095606600460814565L;

	private String key;
	private String value;

	public KeyValuePair(String key, String value) {
		this.setKey(key);
		this.setValue(value);
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public int hashCode() {
		return key.hashCode() + value.hashCode();
	}

}