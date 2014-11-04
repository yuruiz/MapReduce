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
	private int id;
	private WorkerInfo worker;
	private int load;
	private List<Partition> partitions;
	

	public MapTask(int id, WorkerInfo worker, int load) {
		this.id = id;
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

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getLoad() {
		return load;
	}

	public void increaseLoad(int load) {
		this.load = load;
	}
}
