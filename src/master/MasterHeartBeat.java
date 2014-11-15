package master;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import util.Constant;
import util.Log;
import worker.*;

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
			for (WorkerInfo worker : master.getWorkers()) {
				ExecutorService executor = Executors.newSingleThreadExecutor();
				Future<String> future = executor.submit(new PollingThread(
						worker));

				try {
					// Log.log("wait for response, time out: " + timeOut + " "
					// + "port: " + worker.getPollingPort());
					String response = future
							.get(timeOut, TimeUnit.MILLISECONDS);
					if (response == Constant.HEART_BEAT_RESPONSE) {
						master.addWorker(worker);
					}
				} catch (Exception e) {
					System.out.println("Worker time out, remove worker");
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