package task;

import worker.WorkerInfo;

import java.io.Serializable;
import java.util.ArrayList;
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
	// the mappers that have been replaced
	private List<WorkerInfo> replaced;

	// the worker who is executing the reduce task, upon a reducer failure,
	// another worker may take his job
	private WorkerInfo executor;
	private long jobId;
	private int taskId;
	private MapReduceMethod job;
	private int size;

	private int workerId;

	public ReduceTask() {
		replaced = new ArrayList<WorkerInfo>();
	}

	public List<WorkerInfo> getMappers() {
		return mappers;
	}

	public void setMappers(List<WorkerInfo> mappers) {
		this.mappers = mappers;
		size = mappers.size();
	}

	public int getSize() {
		return size;
	}

	public void replaceMapper(WorkerInfo failed, WorkerInfo backup) {
		mappers.remove(failed);
		if (!mappers.contains(backup)) {
			mappers.add(backup);
		}
		replaced.add(failed);
	}

	public List<WorkerInfo> getReplaced() {
		return replaced;
	}

	public WorkerInfo getReducer() {
		return reducer;
	}

	public void setReducer(WorkerInfo reducer) {
		this.reducer = reducer;
		this.executor = reducer;
	}

	public void setExecutor(WorkerInfo executor) {
		this.executor = executor;
	}

	public WorkerInfo getExecutor() {
		return executor;
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

	public MapReduceMethod getMethod() {
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
