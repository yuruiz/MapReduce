package worker;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import task.MapTask;
import task.ReduceTask;

public class WorkerInfo implements Serializable, Comparable<WorkerInfo> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3852873826408845197L;
	private final String ipAddress;
	private final int port;
	private final int pollingPort;
	private int id;
	private transient List<MapTask> mapTasks;
	private transient List<ReduceTask> reduceTasks;

	public WorkerInfo(String ipAddress, int port, int pollingPort) {
		this.ipAddress = ipAddress;
		this.port = port;
		this.pollingPort = pollingPort;
		mapTasks = new ArrayList<MapTask>();
		reduceTasks = new ArrayList<ReduceTask>();
	}

	public int getPort() {
		return port;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getPollingPort() {
		return pollingPort;
	}

	public List<MapTask> getMapTasks() {
		return mapTasks;
	}

	/**
	 * 
	 * @param t
	 */
	public void addMapTask(MapTask t) {
		mapTasks.add(t);
	}

	/**
	 * 
	 * @param e
	 */
	public void addReduceTask(ReduceTask e) {
		reduceTasks.add(e);
	}

	public List<ReduceTask> getReduceTasks() {
		return reduceTasks;
	}

	public void setMapTasks(List<MapTask> mapTasks) {
		this.mapTasks = mapTasks;
	}

	public void setReduceTasks(List<ReduceTask> reduceTasks) {
		this.reduceTasks = reduceTasks;
	}

	public int numTasks() {
		return mapTasks.size() + reduceTasks.size();
	}

	@Override
	public int compareTo(WorkerInfo o) {
		return this.numTasks() - o.numTasks();
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof WorkerInfo)) {
			return false;
		}
		WorkerInfo w = (WorkerInfo) o;
		return w.ipAddress.equals(this.ipAddress);
	}
}
