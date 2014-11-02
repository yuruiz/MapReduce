package util;

import worker.WorkerInfo;

public class InputFile {
	private String fileName;
	private WorkerInfo location;

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public WorkerInfo getLocation() {
		return location;
	}

	public void setLocation(WorkerInfo location) {
		this.location = location;
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
