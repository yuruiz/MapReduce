package task;

import java.util.List;

import util.Partition;
import worker.WorkerInfo;

public class MapTask{
	private WorkerInfo worker;
	private List<Partition> partitions;

	public WorkerInfo getWorker() {
		return worker;
	}

	public void setWorker(WorkerInfo worker) {
		this.worker = worker;
	}

	public List<Partition> getPartitions() {
		return partitions;
	}

	public void setPartitions(List<Partition> partitions) {
		this.partitions = partitions;
	}
}
