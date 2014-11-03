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

	public Set<Partition> getPartitions() {
		return partitions;
	}

}
