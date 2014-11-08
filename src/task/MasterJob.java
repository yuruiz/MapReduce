package task;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import util.Partition;

public class MasterJob {
	private List<MapTask> mappers;
	private List<ReduceTask> reducers;
	private Set<Partition> partitions;
	private MapReduceMethod job;
	private long id;

	public MasterJob() {
		mappers = new ArrayList<MapTask>();
		reducers = new ArrayList<ReduceTask>();
		partitions = new HashSet<Partition>();
	}

	public List<MapTask> getMappers() {
		return mappers;
	}

	public void addMapTask(MapTask map) {
		mappers.add(map);
	}

	public List<ReduceTask> getReducers() {
		return reducers;
	}

	public void addPartition(Partition p) {
		partitions.add(p);
	}

	public void removeMapTask(MapTask t) {
		mappers.remove(t);
	}

	public void removeReduceTask(ReduceTask t) {
		reducers.remove(t);
	}

	public void setReducers(List<ReduceTask> reducers) {
		this.reducers = reducers;
	}

	public Set<Partition> getPartitions() {
		return partitions;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public MapReduceMethod getJob() {
		return job;
	}

	public void setJob(MapReduceMethod job) {
		this.job = job;
	}

}
