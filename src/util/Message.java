package util;

import java.io.Serializable;
import java.util.List;

import task.MapReduceJob;
import task.MapTask;
import task.ReduceTask;

public class Message implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7264137218310503076L;

	private MapReduceJob job;
	private MapTask mapTask;
	private ReduceTask reduceTask;

	public MapReduceJob getJob() {
		return job;
	}

	public void setJob(MapReduceJob job) {
		this.job = job;
	}

	public MapTask getMapTask() {
		return mapTask;
	}

	public void setMapTask(MapTask mapTask) {
		this.mapTask = mapTask;
	}

	public ReduceTask getReduceTask() {
		return reduceTask;
	}

	public void setReduceTask(ReduceTask reduceTask) {
		this.reduceTask = reduceTask;
	}

}