package master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
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
import task.MasterJob.Status;
import util.Config;
import util.InputFile;
import util.Log;
import util.Message;
import util.Message.MessageType;
import util.Partition;

/**
 * The master implementation of the map reduce framework. This class acts as the
 * coordinator of the whole system. The master gets user command from the UI. It
 * then takes care of assign map/reduce tasks. Polling workers and remove failed
 * workers. When a failure is detected, the master handles it and the map reduce
 * result will still be correct unless part of the original data files are not
 * available.
 * 
 * @author siyuwei
 *
 */
public class Master implements Runnable {

	// the thread that is doing polling
	private MasterHeartBeat hearBeat;
	// jobs that is currently running
	private List<MasterJob> runningJobs;
	// all the jobs including those that have finished
	private List<MasterJob> allJobs;
	// job id to job information mapping
	private Map<Long, MasterJob> idToJob;
	// the data file system manager
	private DFSManager manager;
	// all the workers
	private List<WorkerInfo> workers;
	// workers that have failed
	private List<WorkerInfo> failedWorkers;
	// the workers that are currently running
	private List<WorkerInfo> workingWorkers;

	private boolean shutDown;
	private final int port;

	public Master() {
		Config.setup(new String[0]);

		port = Config.MASTER_PORT;
		workers = new ArrayList<WorkerInfo>();
		failedWorkers = new CopyOnWriteArrayList<WorkerInfo>();
		workingWorkers = new CopyOnWriteArrayList<WorkerInfo>();
		runningJobs = new CopyOnWriteArrayList<MasterJob>();
		allJobs = new CopyOnWriteArrayList<MasterJob>();
		hearBeat = new MasterHeartBeat(Config.POLLING_PORT, Config.TIME_OUT,
				Config.SLEEP_TIME);
		hearBeat.setMaster(this);
		idToJob = new ConcurrentHashMap<Long, MasterJob>();
		shutDown = false;
		manager = new DFSManager();

	}

	/**
	 * remove the failed worker from working worker and then handle this failure
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
	 * handle the failed map tasks and reduce tasks
	 * 
	 * @param failed
	 */
	private void handleFailure(WorkerInfo failed) {
		this.handleMapFailure(failed);
		this.handleReduceFailure(failed);

	}

	/**
	 * shut down the server
	 */
	public void shutDown() {
		this.shutDown = true;
	}

	/**
	 * Handle the reduce tasks the failed worker is taking care of
	 * 
	 * @param failedReduce
	 */
	private void handleReduceFailure(WorkerInfo failed) {
		/*
		 * Get all the reduce tasks the failed worker is running
		 */
		List<ReduceTask> failedReduce = failed.getReduceTasks();
		/*
		 * Find backup workers with relatively fewer tasks running
		 */
		List<WorkerInfo> backups = this.getIdleWorkers(failedReduce.size());
		/*
		 * Reassign reduce task to working workers
		 */
		for (int i = 0; i < failedReduce.size(); i++) {
			ReduceTask task = failedReduce.get(i);
			MasterJob job = idToJob.get(task.getJobId());

			if (task.getReplaced().contains(failed)
					|| !runningJobs.contains(job)) {
				continue;
			}
			/*
			 * Reassign this task to another worker
			 */
			WorkerInfo backup = backups.get(i % backups.size());
			Message m = new Message();
			m.setType(MessageType.REDUCE_REQ);
			m.setReduceTask(task);
			/*
			 * Send the task to the back up server
			 */
			try {
				Socket socket = new Socket(backup.getIpAddress(),
						backup.getPort());
				ObjectOutputStream out = new ObjectOutputStream(
						socket.getOutputStream());
				// Log.log("send reduce: " + task.getTaskId() + " "
				// + backup.getPort());
				out.writeObject(m);
				socket.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		// clear the tasks on the failed worker
		failed.setReduceTasks(new ArrayList<ReduceTask>());
		failed.setMapTasks(new ArrayList<MapTask>());

	}

	/**
	 * Reassign the map tasks of the failed worker to other workers
	 * 
	 * @param failed
	 *            the failing worker
	 */
	private void handleMapFailure(WorkerInfo failed) {
		// map tasks that are running on the failed worker
		List<MapTask> failedMap = failed.getMapTasks();
		// get back up workers
		List<WorkerInfo> backups = getIdleWorkers(failedMap.size());
		for (int i = 0; i < failedMap.size(); i++) {
			/*
			 * reassign the failed map tasks to backup workers
			 */
			MapTask t = failedMap.get(i);
			WorkerInfo backup = backups.get(i % backups.size());
			t.setWorker(backup);

			MasterJob job = idToJob.get(t.getJobId());

			/*
			 * if the failed worker is also the reducer for this map task
			 * reassign a reducer
			 */
			if (runningJobs.contains(job)) {
				synchronized (job) {
					job.addMapTask(t);
					for (ReduceTask r : job.getReducers()) {
						if (r.getReducer().equals(failed)) {
							r.setExecutor(backup);
						}
						r.replaceMapper(failed, backup);
					}
				}
			}

			/*
			 * send the map task to the back up worker
			 */
			Message m = new Message();
			m.setType(MessageType.MAP_REQ);
			m.setMapTask(t);
			try {
				Socket socket = new Socket(backup.getIpAddress(),
						backup.getPort());
				ObjectOutputStream out = new ObjectOutputStream(
						socket.getOutputStream());
				out.writeObject(m);
				socket.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	/**
	 * When a worker recovers from failure, add it back to working servers
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
	 * start running the master
	 */
	@Override
	public void run() {
		try {
			/*
			 * first of all, start the heart beat thread
			 */
			new Thread(hearBeat).start();
			/*
			 * open the server socket on master that listens for worker messages
			 */
			ServerSocket server = new ServerSocket(port);
			while (!shutDown) {
				Socket worker = server.accept();
				ObjectOutputStream out = new ObjectOutputStream(
						worker.getOutputStream());
				ObjectInputStream in = new ObjectInputStream(
						worker.getInputStream());
				/*
				 * read a message from worker, start a separate thread for
				 * handling it
				 */
				Message m = (Message) in.readObject();
				new Thread(new MessageHandler(m, worker)).start();
				out.close();

			}
			server.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method takes in a message from worker and handles it regarding the
	 * message type
	 * 
	 * @param m
	 */
	private void handleMessage(Message m) {
		MasterJob job;
		switch (m.getType()) {
		/*
		 * handle the message that a map task has finished
		 */
		case MAP_RES:
			/*
			 * mark the map task as finished, if all map tasks of a job are
			 * finished start sending reduce tasks
			 */
			MapTask map = m.getMapTask();
			job = idToJob.get(map.getJobId());
			job.finishMapTask(map);
			if (job.allMapFinished() && job.status == Status.running) {
				this.sendReduceTask(job);
			}
			break;
		/*
		 * handle the message that a reduce task has finished
		 */
		case REDUCE_RES:
			/*
			 * mark the reduce task as finished, if all reduce tasks are
			 * finished, remove the job from running jobs
			 */
			ReduceTask reduce = m.getReduceTask();
			job = idToJob.get(reduce.getJobId());
			job.finishReduceTask(reduce);
			System.out.println(m.getResult());
			if (job.allMapFinished() && job.allReduceFinished()) {
				runningJobs.remove(job);
				job.status = Status.finished;
				/*
				 * Upon a job finished, remove this map task from the worker's
				 * list
				 */
				for (MapTask t : job.getMappers()) {
					for (WorkerInfo w : workingWorkers) {
						if (w.equals(t.getWorker())) {
							synchronized (w) {
								w.removeMapTask(t);
							}
						}
					}
				}
				/*
				 * Upon a job finished, remove this reduce task from the
				 * worker's list
				 */
				for (ReduceTask t : job.getReducers()) {
					for (WorkerInfo w : workingWorkers) {
						if (w.equals(t.getExecutor())) {
							synchronized (w) {
								w.removeReduceTask(t);
							}
						}
					}
				}

			}
			break;
		case WORKER_REG:
			/*
			 * a worker indicates that it wants to join the group, add it to the
			 * group of workers. Add the data files the worker reports to the
			 * file pool
			 */
			WorkerInfo newWorker = m.getWorker();

			Log.log("worker registered:" + newWorker.getId() + " "
					+ newWorker.getPort());

			synchronized (workingWorkers) {
				if (!workingWorkers.contains(newWorker)) {
					workingWorkers.add(newWorker);
				}
				if (!workers.contains(newWorker)) {
					workers.add(newWorker);
				}
			}

			/*
			 * Initialize the new worker configuration
			 */
			newWorker.setMapTasks(new ArrayList<MapTask>());
			newWorker.setReduceTasks(new ArrayList<ReduceTask>());
			List<InputFile> inputFiles = m.getInputs();

			if (inputFiles == null) {
				break;
			}
			for (InputFile f : inputFiles) {
				manager.addFile(f.getFileName(), newWorker, f.getLength());
			}
			break;
		default:
			break;
		}

	}

	/**
	 * The message handler class that will call master's handling method and run
	 * it on a separate thread
	 * 
	 * @author siyuwei
	 *
	 */
	private class MessageHandler implements Runnable {

		private Message m;
		private Socket socket;

		public MessageHandler(Message m, Socket socket) {
			this.m = m;
			this.socket = socket;
		}

		@Override
		public void run() {
			// handle the message and then closes the socket
			handleMessage(m);
			try {
				socket.close();
			} catch (IOException e) {
				// ignore close error
				e.printStackTrace();
			}
		}

	}

	/**
	 * starts a new job execution required by the client which contains map and
	 * reduce tasks
	 * 
	 * @param job
	 */
	public void newJob(ClientJob job) {

		if (workingWorkers.size() == 0) {
			Log.log("Zero workers have registered, job cannot be run, try later");
			return;
		}
		ServiceHandler handler = new ServiceHandler(job);
		new Thread(handler).start();

	}

	/**
	 * return the list of running workers
	 * 
	 * @return the running worker list
	 */
	public List<WorkerInfo> getWorkers() {
		return workingWorkers;
	}

	/**
	 * The thread that handles a client job
	 * 
	 * @author siyuwei
	 *
	 */
	private class ServiceHandler implements Runnable {

		private ClientJob job;

		public ServiceHandler(ClientJob job) {
			this.job = job;
		}

		/**
		 * divide the source data and assign the map tasks to workers
		 */
		@Override
		public void run() {
			List<Partition> allPartitions = new ArrayList<Partition>();
			int load = divideData(allPartitions, job);
			assignMapTask(allPartitions, job, load);

		}
	}

	/**
	 * Stop a job with the given id
	 * 
	 * @param id
	 */
	public void stopJob(long id) {
		MasterJob j = idToJob.get(id);
		if (j == null) {
			System.out.println("Unrecognized job id");
		} else {
			if (j.status == Status.running) {
				j.status = Status.stopped;
			}
		}
	}

	/**
	 * Assign the map tasks to all the working workers available
	 * 
	 * @param partitions
	 *            all the data partitions
	 * @param job
	 *            the client job which defined the map/reduce method
	 * @param load
	 *            expected record load on a mapper
	 */
	public void assignMapTask(List<Partition> partitions, ClientJob job,
			int load) {

		/*
		 * predefine the reducers for this job, achieve relative optimal load
		 * balance
		 */
		List<WorkerInfo> reducers = this.getIdleWorkers(job.getMaxReduceFile());

		synchronized (workingWorkers) {

			/*
			 * Assign job id by the current time in milliseconds, guarantee no
			 * duplicate id
			 */
			long jobId = System.currentTimeMillis();

			/*
			 * Initialize the reduce task settings
			 */
			List<ReduceTask> tasks = new ArrayList<ReduceTask>();
			int reduceBaseId = 0;
			for (WorkerInfo reducer : reducers) {
				/*
				 * create a new reduce task, set properties
				 */
				ReduceTask r = new ReduceTask();
				r.setMappers(new ArrayList<WorkerInfo>(workingWorkers));
				r.setReducer(reducer);
				r.setJobId(jobId);
				r.setTaskId(reduceBaseId++);
				r.setJob(job);
				r.setWorkerId(reducer.getId());
				reducer.addReduceTask(r);
				tasks.add(r);
			}
			/*
			 * Create a new master job for master to track
			 */
			System.out.println("New job created, ID: " + jobId);
			MasterJob masterJob = new MasterJob();
			masterJob.setId(jobId);
			masterJob.setReducers(tasks);
			masterJob.setJob(job);
			runningJobs.add(masterJob);
			allJobs.add(masterJob);
			idToJob.put(masterJob.getId(), masterJob);
			int baseId = 0;
			/*
			 * Create a new map task for each worker
			 */
			for (WorkerInfo worker : workingWorkers) {
				/*
				 * create a new map task, set the properties
				 */
				MapTask t = new MapTask(masterJob.getId(), worker, 0);
				t.setTaskId(baseId);
				t.setMethod(job);
				baseId++;
				t.setReducers(reducers);
				masterJob.addMapTask(t);
				worker.addMapTask(t);

			}

			/*
			 * Assign tasks only if word is not stopped
			 */
			if (masterJob.status == Status.stopped) {
				return;
			}
			/*
			 * Assign partitions to each map task
			 */
			int i = 0;
			for (MapTask task : masterJob.getMappers()) {

				/*
				 * keep assigning partitions when the amount of records of this
				 * work has not reached average
				 */
				while (task.getLoad() <= load) {
					if (i >= partitions.size()) {
						break;
					}
					Partition p = partitions.get(i);
					task.addPartition(p);
					task.increaseLoad(p.getLength());
					i++;

				}

				/*
				 * Send the map task to workers for execution
				 */
				Message message = new Message();
				message.setType(MessageType.MAP_REQ);
				message.setMapTask(task);
				try {
					// send the map request to each mapper
					Socket toWorker = new Socket(task.getWorker()
							.getIpAddress(), task.getWorker().getPort());
					ObjectOutputStream out = new ObjectOutputStream(
							toWorker.getOutputStream());
					out.writeObject(message);
					toWorker.close();
				} catch (Exception e) {
					e.printStackTrace();
				}

			}

		}
	}

	/**
	 * Upon receiving the confirmation that all maps are finished, start sending
	 * reduce tasks to reducers
	 * 
	 * @param jobId
	 */
	private void sendReduceTask(MasterJob job) {
		List<ReduceTask> tasks = job.getReducers();

		for (ReduceTask task : tasks) {
			WorkerInfo reducer = task.getExecutor();
			try {
				/*
				 * Send the reduce task to each reducer for execution
				 */

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
				e.printStackTrace();
			} catch (IOException e) {
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
			/*
			 * Build a priority queue according to the amount of tasks that is
			 * currently running on a worker
			 */
			num = Math.min(num, workingWorkers.size());
			List<WorkerInfo> idleWorkers = new ArrayList<WorkerInfo>();
			PriorityQueue<WorkerInfo> queue = new PriorityQueue<WorkerInfo>();
			queue.addAll(workingWorkers);
			/*
			 * retrieve the nth idlest workers
			 */
			for (int i = 0; i < num; i++) {
				idleWorkers.add(queue.poll());
			}
			return idleWorkers;
		}
	}

	/**
	 * for all the data of a given client job， divide them evenly into
	 * partitions for assigning to workers
	 * 
	 * @param list
	 *            the list of assigned partitions
	 * @param job
	 *            client job
	 * @return expected amount of record on each worker
	 */
	public int divideData(List<Partition> list, ClientJob job) {
		List<String> files = job.getfiles();
		List<InputFile> data = new ArrayList<InputFile>();
		int total = 0;

		for (String file : files) {
			InputFile input = manager.getFile(file);
			if (input == null) {
				/*
				 * if a file is not in the file system, skip it as we have no
				 * way going around it, task will still be running but
				 * correctness cannot be guaranteed
				 */
				continue;
			} else {
				// System.out.println(input.getFileName());
				data.add(input);
				total += input.getLength();
			}
		}

		/*
		 * expected load of every worker is the average
		 */
		int expectedSize = total / workingWorkers.size();
		// Log.log("total: " + total + "average: " + expectedSize);

		for (InputFile file : data) {
			int size = file.getLength();

			if (size <= expectedSize) {
				// For a file smaller than expected size, make it a separate
				// size
				Partition p = new Partition();
				p.setFileName(file.getFileName());
				p.setOwners(file.getLocations());
				p.setStartIndex(0);
				p.setEndIndex(size - 1);
				list.add(p);
				continue;
			}

			while (size > 0) {
				/*
				 * if a file is large, divide it into separate partitions
				 */
				Partition p = new Partition();
				p.setFileName(file.getFileName());
				p.setOwners(file.getLocations());
				p.setEndIndex(size - 1);
				p.setStartIndex(Math.max(0, size - expectedSize));
				list.add(p);
				size -= expectedSize;
			}

		}

		// System.out.println("num of partitions: " + list.size());

		return expectedSize;

	}

	/**
	 * print out the status of all the jobs
	 */
	public void showJobStatus() {
		System.out.println("**** Job status: ****");
		for (MasterJob j : allJobs) {
			System.out.print("Job Id: " + j.getId() + "\t");
			System.out.print("Status: " + j.status + "\t");
			System.out.print("Num of map tasks: " + j.mapTaskSize() + "\t");
			System.out.print("Num of reduce tasks: " + j.reduceTaskSize());
			System.out.print("Map tasks remain running: "
					+ j.getMappers().size() + "\t");
			System.out.print("Reuce tasks remian running: "
					+ j.getReducers().size() + "\n");
		}

		System.out.println("**** end ****");
	}
}
