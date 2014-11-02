package util;

import java.io.Serializable;
import java.util.List;

import task.MapReduceJob;

public class Message implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7264137218310503076L;
	
	private MapReduceJob job;
	private List<Partition> partitions;

	public MapReduceJob getJob() {
		return job;
	}

	public void setJob(MapReduceJob job) {
		this.job = job;
	}
	
	
}