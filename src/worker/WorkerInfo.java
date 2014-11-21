package worker;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import task.MapTask;
import task.ReduceTask;

/**
 * This class represents all the information of a worker including IP address,
 * port number and the tasks the worker is taking care of.
 * 
 * @author siyuwei
 *
 */
public class WorkerInfo implements Serializable, Comparable<WorkerInfo> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3852873826408845197L;
	private final String ipAddress;
	// port for connection with master
	private final int port;
	// port for heart beat message
	private final int pollingPort;
	private int id;
	/*
	 * the tasks that are running on this worker, they are marked as transient
	 * as it's only for master tracking usage
	 */
	private transient List<MapTask> mapTasks;
	private transient List<ReduceTask> reduceTasks;

	/**
	 * Initialize the information of a worker
	 * 
	 * @param ipAddress
	 *            address
	 * @param port
	 *            port for connection
	 * @param pollingPort
	 *            port for polling
	 */
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

	public void removeMapTask(MapTask t) {
		mapTasks.remove(t);
	}

	public void removeReduceTask(ReduceTask t) {
		reduceTasks.remove(t);
	}

	public int getPollingPort() {
		return pollingPort;
	}

	public List<MapTask> getMapTasks() {
		return mapTasks;
	}

	/**
	 * Add a map task to this worker
	 * 
	 * @param t
	 *            the task
	 */
	public void addMapTask(MapTask t) {
		mapTasks.add(t);
	}

	/**
	 * Add a reduce task to this worker
	 * 
	 * @param e
	 *            the task
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
		return w.ipAddress.equals(this.ipAddress) && w.getPort() == this.port;
	}

	@Override
	public int hashCode() {
		return ipAddress.hashCode() + port;
	}

}
