package master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import worker.*;
import task.ClientJob;
import task.MapTask;
import task.MasterJob;
import task.ReduceTask;
import util.Config;
import util.InputFile;
import util.Message;
import util.Message.MessageType;
import util.Partition;

public class Master implements Runnable {

	private MasterHeartBeat hearBeat;
	private List<MasterJob> runningJobs;
	private Map<Long, MasterJob> idToJob;
	private DFSManager manager;
	private List<WorkerInfo> workers;
	private List<WorkerInfo> failedWorkers;
	private List<WorkerInfo> workingWorkers;
	private boolean shutDown;

	private Deque<ClientJob> toBeDone;

	private final int port;

	public Master() {
		Config.setup(new String[0]);

		port = Config.MASTER_PORT;
		workers = new ArrayList<WorkerInfo>();
		failedWorkers = new CopyOnWriteArrayList<WorkerInfo>();
		workingWorkers = new CopyOnWriteArrayList<WorkerInfo>();
		runningJobs = new CopyOnWriteArrayList<MasterJob>();
		hearBeat = new MasterHeartBeat(Config.POLLING_PORT, Config.TIME_OUT,
				Config.SLEEP_TIME);
		hearBeat.setMaster(this);
		idToJob = new ConcurrentHashMap<Long, MasterJob>();
		shutDown = false;

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
		handleFailure(failed);
	}

	/**
	 * 
	 * @param w
	 */
	private void handleFailure(WorkerInfo w) {
		List<MapTask> failedMap = w.getMapTasks();
		w.setMapTasks(new ArrayList<MapTask>());
		this.handleMapFailure(failedMap);
		List<ReduceTask> failedReduce = w.getReduceTasks();
		w.setReduceTasks(new ArrayList<ReduceTask>());
		this.handleReduceFailure(failedReduce);

	}

	public void shutDown() {
		this.shutDown = true;
	}

	/**
	 * 
	 * @param failedReduce
	 */
	private void handleReduceFailure(List<ReduceTask> failedReduce) {
		List<WorkerInfo> backups = this.getIdleWorkers(failedReduce.size());
		/*
		 * Reassign reduce task to working workers
		 */
		for (int i = 0; i < failedReduce.size(); i++) {
			WorkerInfo backup = backups.get(i % backups.size());
			ReduceTask task = failedReduce.get(i);
			task.setReducer(backup);
			Message m = new Message();
			m.setType(MessageType.REDUCE_REQ);
			m.setReduceTask(task);
			try {
				Socket socket = new Socket(backup.getIpAddress(),
						backup.getPort());
				ObjectOutputStream out = new ObjectOutputStream(
						socket.getOutputStream());
				out.writeObject(m);
				socket.close();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for (WorkerInfo w : task.getMappers()) {

			}
		}
	}

	private void handleMapFailure(List<MapTask> failedMap) {
		List<WorkerInfo> backups = getIdleWorkers(failedMap.size());
		for (int i = 0; i < failedMap.size(); i++) {
			MapTask t = failedMap.get(i);
			WorkerInfo backup = backups.get(i % backups.size());
			t.setWorker(backup);
			Message m = new Message();
			m.setType(MessageType.MAP_REQ);
			try {
				Socket socket = new Socket(backup.getIpAddress(),
						backup.getPort());
				ObjectOutputStream out = new ObjectOutputStream(
						socket.getOutputStream());
				out.writeObject(m);
				socket.close();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

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

	public void run() {
		try {
			ServerSocket server = new ServerSocket(port);
			while (!shutDown) {
				Socket worker = server.accept();
				ObjectOutputStream out = new ObjectOutputStream(
						worker.getOutputStream());
				ObjectInputStream in = new ObjectInputStream(
						worker.getInputStream());
				Message m = (Message) in.readObject();
				new Thread(new MessageHandler(m)).start();
				out.close();
				server.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void handleMessage(Message m) {
		MasterJob job;
		switch (m.getType()) {
		case MAP_RES:
			MapTask map = m.getMapTask();
			job = idToJob.get(map.getJobId());
			job.finishMapTask(map);
			if (job.allMapFinished()) {
				this.sendReduceTask(job);
			}
			break;
		case REDUCE_RES:
			ReduceTask reduce = m.getReduceTask();
			job = idToJob.get(reduce.getJobId());
			job.finishReduceTask(reduce);
			if (job.allMapFinished() && job.allReduceFinished()) {
				runningJobs.remove(job);
			}
			break;
		case WORKER_REG:
			WorkerInfo newWorker = m.getReceiver();
			
			break;
		default:
			break;
		}

	}

	private class MessageHandler implements Runnable {

		private Message m;

		public MessageHandler(Message m) {
			this.m = m;
		}

		@Override
		public void run() {
			handleMessage(m);
		}

	}

	/**
	 * 
	 */
	public void newJob(ClientJob job) {

		ServiceHandler handler = new ServiceHandler(job);
		new Thread(handler).start();

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

		public ServiceHandler(ClientJob job) {
			this.job = job;

		}

		@Override
		public void run() {
			List<Partition> allPartitions = new ArrayList<Partition>();
			int load = divideData(allPartitions, job);
			assignMapTask(allPartitions, job, load);

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

		List<WorkerInfo> reducers = this.getIdleWorkers(job.getMaxReduceFile());

		synchronized (workingWorkers) {

			long jobId = System.currentTimeMillis();

			List<ReduceTask> tasks = new ArrayList<ReduceTask>();
			int reduceBaseId = 0;
			for (WorkerInfo reducer : reducers) {
				ReduceTask r = new ReduceTask();
				r.setMappers(workingWorkers);
				r.setReducer(reducer);
				r.setJobId(jobId);
				r.setTaskId(reduceBaseId++);
				r.setJob(job);
				r.setWorkerId(reducer.getId());
				reducer.addReduceTask(r);
				tasks.add(r);
			}
			/*
			 * Create a new job
			 */
			MasterJob masterJob = new MasterJob();
			masterJob.setId(jobId);
			masterJob.setReducers(tasks);
			masterJob.setJob(job);
			runningJobs.add(masterJob);
			idToJob.put(masterJob.getId(), masterJob);
			int baseId = 0;
			/*
			 * Create a new map task for each worker
			 */
			for (WorkerInfo worker : workingWorkers) {

				MapTask t = new MapTask(masterJob.getId(), worker, load);
				t.setTaskId(baseId);
				t.setMethod(job);
				baseId++;
				t.setReducers(reducers);
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
				message.setType(MessageType.MAP_REQ);
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
	 * Upon receiving the confirmation that all maps are finished, start
	 * reducing
	 * 
	 * @param jobId
	 */
	private void sendReduceTask(MasterJob job) {
		List<ReduceTask> tasks = job.getReducers();
		for (ReduceTask task : tasks) {
			WorkerInfo reducer = task.getReducer();
			try {
				Socket socket = new Socket(reducer.getIpAddress(),
						reducer.getPort());
				ObjectOutputStream out = new ObjectOutputStream(
						socket.getOutputStream());
				Message m = new Message();
				m.setType(MessageType.REDUCE_REQ);
				m.setJobId(task.getJobId());
				m.setReduceTask(task);
				out.writeObject(m);
				socket.close();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	/**
	 * Assign the workers with the least amount of tasks running as the
	 * relatively idle workers
	 * 
	 * @return
	 */
	private List<WorkerInfo> getIdleWorkers(int num) {
		synchronized (workingWorkers) {
			num = Math.min(num, workingWorkers.size());
			List<WorkerInfo> idleWorkers = new ArrayList<WorkerInfo>();
			PriorityQueue<WorkerInfo> queue = new PriorityQueue<WorkerInfo>();
			queue.addAll(workingWorkers);
			for (int i = 0; i < num; i++) {
				idleWorkers.add(queue.poll());
			}
			return idleWorkers;
		}
	}

	public int divideData(List<Partition> list, ClientJob job) {
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
			int size = file.getLength();

			// For a file smaller
			if (size <= expectedSize) {
				Partition p = new Partition();
				p.setFileName(file.getFileName());
				p.setOwners(file.getLocations());
				p.setStartIndex(0);
				p.setEndIndex(size - 1);
				continue;
			}

			while (size > 0) {
				Partition p = new Partition();
				p.setFileName(file.getFileName());
				p.setOwners(file.getLocations());
				p.setEndIndex(size - 1);
				p.setStartIndex(Math.max(0, size - expectedSize));
				size -= expectedSize;
			}

		}

		return expectedSize;

	}

	public static void main() {

	}

}
