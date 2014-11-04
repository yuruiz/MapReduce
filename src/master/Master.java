package master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import worker.*;
import task.ClientJob;
import task.MapTask;
import task.MasterJob;
import util.Config;
import util.InputFile;
import util.Message;
import util.Partition;

public class Master {

	private MasterHeartBeat hearBeat;
	private List<MasterJob> runningJobs;
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
		runningJobs = new CopyOnWriteArrayList<MasterJob>();
		hearBeat = new MasterHeartBeat(Config.POLLING_PORT, Config.TIME_OUT,
				Config.SLEEP_TIME);
		hearBeat.setMaster(this);

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
	 * @param w
	 */
	private void handleFailure(WorkerInfo w) {

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

	/**
	 * Assign the map tasks to all the working workers
	 * 
	 * @param partitions
	 * @param job
	 * @param load
	 */
	public void assignMapTask(List<Partition> partitions, ClientJob job,
			int load) {
		synchronized (workingWorkers) {
			/*
			 * Create a new job
			 */
			MasterJob masterJob = new MasterJob();
			masterJob.setId(System.currentTimeMillis());
			runningJobs.add(masterJob);
			/*
			 * Create a new map task for each worker
			 */
			for (WorkerInfo worker : workingWorkers) {

				MapTask t = new MapTask(masterJob.getId(), worker, load);
				masterJob.addMapTask(t);
				worker.addMapTask(t);

			}

			/*
			 * Assign partitions to each map task
			 */
			int i = 0;
			outer: for (MapTask task : masterJob.getMappers()) {

				while (task.getLoad() < load) {
					Partition p = partitions.get(i);
					task.addPartition(p);
					task.increaseLoad(p.getLength());

					if (i >= partitions.size()) {
						break outer;
					}
				}

				/*
				 * Send the map task to workers for execution
				 */
				Message message = new Message();
				message.setJob(job);
				message.setMapTask(task);
				try {
					Socket toWorker = new Socket(task.getWorker()
							.getIpAddress(), task.getWorker().getPort());
					ObjectOutputStream out = new ObjectOutputStream(
							toWorker.getOutputStream());
					out.writeObject(message);
					toWorker.close();
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}
	}

	/**
	 * Assign the workers with the least tasks to do as the reducers
	 * 
	 * @return
	 */
	private synchronized List<WorkerInfo> getReducers(int num) {
		num = Math.min(num, workingWorkers.size());
		List<WorkerInfo> reducers = new ArrayList<WorkerInfo>();
		PriorityQueue<WorkerInfo> queue = new PriorityQueue<WorkerInfo>();
		queue.addAll(workingWorkers);
		for (int i = 0; i < num; i++) {
			reducers.add(queue.poll());
		}
		return reducers;
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