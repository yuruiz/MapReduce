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
		final int prime = 31;
		int result = 1;
		result = prime * result + (key == null ? 0 : key.hashCode());
		result = prime * result + (value == null ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof KeyValuePair)) {
			return false;
		}
		KeyValuePair other = (KeyValuePair) o;
		return equals(key, other.key) && equals(value, other.value);
	}

	private static boolean equals(Object o1, Object o2) {
		return o1 == null ? o2 == null : o1.equals(o2);
	}

}