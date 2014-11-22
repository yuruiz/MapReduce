package task;

import java.util.ArrayList;
import java.util.List;

/**
 * The job abstraction on the master's side. Each job has a unique id. It keeps
 * tracking of all the running/finished map/reduce tasks.
 * 
 * @author siyuwei
 *
 */
public class MasterJob {

	// running map tasks
	private List<MapTask> mappers;
	// running reduce tasks
	private List<ReduceTask> reducers;
	// finished map tasks
	private List<MapTask> finishedMaps;
	// finished reduce tasks
	private List<ReduceTask> finishedReduces;

	private MapReduceMethod job;
	private long id;
	public Status status;

	public enum Status {
		running, finished, stopped
	}

	public MasterJob() {
		mappers = new ArrayList<MapTask>();
		reducers = new ArrayList<ReduceTask>();
		finishedMaps = new ArrayList<MapTask>();
		finishedReduces = new ArrayList<ReduceTask>();
		status = Status.running;

	}

	public void finishMapTask(MapTask map) {
		if (mappers.contains(map)) {
			mappers.remove(map);
			finishedMaps.add(map);
		}
	}

	/**
	 * 
	 * @param reduce
	 */
	public void finishReduceTask(ReduceTask reduce) {
		if (reducers.contains(reduce)) {
			reducers.remove(reduce);
			finishedReduces.add(reduce);
		}
	}

	/**
	 * check if all map tasks are finished
	 * 
	 * @return
	 */
	public boolean allMapFinished() {
		return mappers.size() == 0;
	}

	/**
	 * Check if all reduce tasks are finished
	 * 
	 * @return
	 */
	public boolean allReduceFinished() {
		return reducers.size() == 0;
	}

	public List<MapTask> getMappers() {
		return mappers;
	}

	public void addMapTask(MapTask map) {
		if (!mappers.contains(map)) {
			mappers.add(map);
		}
	}

	public List<ReduceTask> getReducers() {
		return reducers;
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

	/**
	 * Return the total size of map tasks of the job regardless of finished or
	 * not
	 * 
	 * @return
	 */
	public int mapTaskSize() {
		return mappers.size() + finishedMaps.size();
	}

	/**
	 * Return the total size of reduce tasks of the job regardless of finished
	 * or not
	 */
	public int reduceTaskSize() {
		return reducers.size() + finishedReduces.size();
	}

}
