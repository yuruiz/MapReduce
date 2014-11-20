package util;

import java.io.Serializable;
import java.util.List;

import worker.WorkerInfo;

/**
 * This class represents a file abstraction in the system. As a file might be
 * replicated by several different workers. A list of locations is stored. Also,
 * a file has information about the amount of records on it.
 * 
 * @author siyuwei
 *
 */
public class InputFile implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3905404058646869761L;
	private String fileName;
	private List<WorkerInfo> locations;
	private int length;

	public InputFile(String fileName, List<WorkerInfo> locations, int length) {
		this.fileName = fileName;
		this.locations = locations;
		this.length = length;
	}

	public InputFile(String fileName) {
		this.fileName = fileName;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public List<WorkerInfo> getLocations() {
		return locations;
	}

	public void addLocation(WorkerInfo w) {
		this.locations.add(w);
	}

	public void setLocations(List<WorkerInfo> locations) {
		this.locations = locations;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof InputFile)) {
			return false;
		} else {
			InputFile file = (InputFile) o;
			return this.fileName.equals(file.getFileName());
		}
	}

	@Override
	public int hashCode() {
		return this.fileName.hashCode();
	}

}
