package master;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import util.Constant;
import util.Log;
import worker.*;

/**
 * The class that is taking care of polling the slaves periodically, when it
 * detects that a slave is not responding in the given time out limit. It
 * removes the slave from the working workers. The master will then handle this
 * failure issue.
 * 
 * @author siyuwei
 *
 */
public class MasterHeartBeat implements Runnable {

	private final int timeOut;
	private final int sleepTime;
	protected boolean shutDown = false;

	private Master master;

	public MasterHeartBeat(int pollingPort, int timeOut, int sleepTime) {
		this.timeOut = timeOut;
		this.sleepTime = sleepTime;
	}

	public void setMaster(Master master) {
		this.master = master;
	}

	@Override
	public void run() {
		while (!shutDown) {
			/*
			 * poll each worker periodically
			 */
			for (WorkerInfo worker : master.getWorkers()) {
				ExecutorService executor = Executors.newSingleThreadExecutor();
				Future<String> future = executor.submit(new PollingThread(
						worker));

				try {

					String response = future
							.get(timeOut, TimeUnit.MILLISECONDS);
					if (response == Constant.HEART_BEAT_RESPONSE) {
						master.addWorker(worker);
					}
				} catch (Exception e) {
					// when time out or other exception happens
					Log.log("Worker time out, remove worker");
					master.removeWorker(worker);
				}
			}

			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// Ignore this
			}
		}
	}

	private static class PollingThread implements Callable<String> {

		private WorkerInfo worker;

		public PollingThread(WorkerInfo worker) {
			this.worker = worker;
		}

		@Override
		public String call() throws Exception {
			/*
			 * this thread sends a query to a worker and wait for it to response
			 */
			Socket poll = new Socket(worker.getIpAddress(),
					worker.getPollingPort());
			PrintWriter writer = new PrintWriter(poll.getOutputStream(), true);
			writer.println(Constant.HEART_BEAT_QUERY);

			Scanner s = new Scanner(poll.getInputStream());
			String response = s.nextLine();
			s.close();
			poll.close();
			return response;
		}

	}
}