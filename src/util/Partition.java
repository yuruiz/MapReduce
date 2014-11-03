package util;

import java.io.Serializable;
import java.util.List;

import worker.WorkerInfo;

public class Partition implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6776291270280481160L;
	private int startIndex;
	private int endIndex;
	private String fileName;
	private List<WorkerInfo> owners;

	public int getLength() {
		return endIndex - startIndex + 1;
	}

	public int getStartIndex() {
		return startIndex;
	}

	public void setStartIndex(int startIndex) {
		this.startIndex = startIndex;
	}

	public int getEndIndex() {
		return endIndex;
	}

	public void setEndIndex(int endIndex) {
		this.endIndex = endIndex;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public List<WorkerInfo> getOwners() {
		return owners;
	}

	public void setOwners(List<WorkerInfo> owners) {
		this.owners = owners;
	}
}
