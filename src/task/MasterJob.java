package task;

import java.util.List;
import java.util.Set;

import util.Partition;

public class MasterJob {
	private List<MapTask> mappers;
	private List<ReduceTask> reducers;
	private Set<Partition> partitions;
}
