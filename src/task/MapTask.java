package task;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import util.Partition;
import worker.WorkerInfo;

public class MapTask implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5483890248653777591L;
	private long jobId;
	private int taskId;
	private int load;
	private MapReduceJob job;

	private WorkerInfo worker;
	private List<WorkerInfo> reducers;
	private List<Partition> partitions;

	public MapTask(long id, WorkerInfo worker, int load) {
		this.jobId = id;
		this.worker = worker;
		this.load = load;
		partitions = new ArrayList<Partition>();
	}

	public WorkerInfo getWorker() {
		return worker;
	}

	public void setWorker(WorkerInfo worker) {
		this.worker = worker;
	}

	public List<Partition> getPartitions() {
		return partitions;
	}

	public void addPartition(Partition p) {
		this.partitions.add(p);
	}

	public long getJobId() {
		return jobId;
	}

	public void setJobId(long id) {
		this.jobId = id;
	}

	public int getLoad() {
		return load;
	}

	public void increaseLoad(int load) {
		this.load = load;
	}

	public List<WorkerInfo> getReducers() {
		return reducers;
	}

	public void setReducers(List<WorkerInfo> reducers) {
		this.reducers = reducers;
	}

	public int getTaskId() {
		return taskId;
	}

	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof MapTask)) {
			return false;
		}
		MapTask t = (MapTask) o;
		return this.jobId == t.jobId && this.taskId == t.taskId;

	}

	@Override
	public int hashCode() {
		return (int) jobId + taskId;
	}

	public MapReduceJob getJob() {
		return job;
	}

	public void setJob(MapReduceJob job) {
		this.job = job;
	}
}
