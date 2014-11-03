package master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import worker.*;
import task.ClientJob;
import task.MapTask;
import task.MasterJob;
import util.Config;
import util.InputFile;
import util.Partition;

public class Master {

	private MasterHeartBeat hearBeat;
	private List<ClientJob> runningJobs;
	private DFSManager manager;
	private List<WorkerInfo> workers;
	private List<WorkerInfo> failedWorkers;
	private List<WorkerInfo> workingWorkers;

	private Deque<ClientJob> toBeDone;

	private final int port = Config.MASTER_PORT;

	public Master() {
		workers = new ArrayList<WorkerInfo>();
		failedWorkers = new CopyOnWriteArrayList<WorkerInfo>();
		workingWorkers = new CopyOnWriteArrayList<WorkerInfo>();
	}

	/**
	 * 
	 * @param failed
	 */
	public synchronized void removeWorker(WorkerInfo failed) {
		workingWorkers.remove(failed);
		if (!failedWorkers.contains(failed)) {
			failedWorkers.add(failed);
		}
	}

	/**
	 * 
	 * @param recovered
	 */
	public synchronized void addWorker(WorkerInfo recovered) {
		if (!workingWorkers.contains(recovered)) {
			workingWorkers.add(recovered);
		}
		failedWorkers.remove(recovered);
	}

	/**
	 * 
	 */
	public void start() {
		try {
			ServerSocket server = new ServerSocket(port);
			while (true) {
				Socket client = server.accept();
				ObjectOutputStream out = new ObjectOutputStream(
						client.getOutputStream());
				ObjectInputStream in = new ObjectInputStream(
						client.getInputStream());
				ClientJob job = (ClientJob) in.readObject();
				ServiceHandler handler = new ServiceHandler(job, client, out);
				new Thread(handler).start();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @return
	 */
	public List<WorkerInfo> getWorkers() {
		return workers;
	}

	private class ServiceHandler implements Runnable {

		private ClientJob job;
		private Socket client;
		private ObjectOutputStream out;

		public ServiceHandler(ClientJob job, Socket client,
				ObjectOutputStream out) {
			this.job = job;
			this.client = client;
			this.out = out;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub

		}
	}

	public void assignMapTask(List<Partition> partitions, ClientJob job,
			int load) {
		synchronized (workingWorkers) {
			MasterJob masterJob = new MasterJob();
			int id = 0;
			for (WorkerInfo worker : workingWorkers) {
				masterJob.addMapTask(new MapTask(id, worker, load));
			}
			int i = 0;
			for (MapTask task : masterJob.getMappers()) {
				if (i >= partitions.size()) {
					break;
				}
				while (task.getLoad() < load) {
					Partition p = partitions.get(i);
					task.addPartition(p);
					task.increaseLoad(p.getLength());
				}
			}
		}
	}

	public List<Partition> divideData(ClientJob job) {
		List<String> files = job.getfiles();
		List<InputFile> data = new ArrayList<InputFile>();
		int total = 0;

		for (String file : files) {
			InputFile input = manager.getFile(file);
			if (input == null) {
				// TODO Handle exception
			} else {
				data.add(input);
				total += input.getLength();
			}
		}

		int expectedSize = total / workingWorkers.size();

		for (InputFile file : data) {

		}

		return null;

	}

	public static void main() {

	}

}