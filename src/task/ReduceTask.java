package task;

import java.io.Serializable;
import java.util.List;

import worker.WorkerInfo;

public class ReduceTask implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8451457993871474773L;

	// The reducer should know which workers to get data from
	private List<WorkerInfo> mappers;
	// The reduce task has a reducer that's doing the task
	private WorkerInfo reducer;
	private long jobId;
	private int taskId;
	private MapReduceJob job;

	public List<WorkerInfo> getMappers() {
		return mappers;
	}

	public void setMappers(List<WorkerInfo> mappers) {
		this.mappers = mappers;
	}

	public WorkerInfo getReducer() {
		return reducer;
	}

	public void setReducer(WorkerInfo reducer) {
		this.reducer = reducer;
	}

	public long getJobId() {
		return jobId;
	}

	public void setJobId(long jobId) {
		this.jobId = jobId;
	}

	public int getTaskId() {
		return taskId;
	}

	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}

	public MapReduceJob getJob() {
		return job;
	}

	public void setJob(MapReduceJob job) {
		this.job = job;
	}

}
