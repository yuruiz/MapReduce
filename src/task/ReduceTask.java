package task;

import worker.WorkerInfo;

import java.io.Serializable;
import java.util.List;

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
	private MapReduceMethod job;
	// the id of the worker who is executing the reduce task, upon a reducer
	// failure, another worker may take his job by taking the failed node's id
	private int workerId;

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

	public MapReduceMethod getJob() {
		return job;
	}

	public void setJob(MapReduceMethod job) {
		this.job = job;
	}

	public int getWorkerId() {
		return workerId;
	}

	public void setWorkerId(int workerId) {
		this.workerId = workerId;
	}

}
