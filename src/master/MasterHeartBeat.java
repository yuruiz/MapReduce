package master;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import worker.*;

public class MasterHeartBeat implements Runnable {
	private final int pollingPort;

	private List<WorkerInfo> workers;
	private List<WorkerInfo> failedWorkers;
	private List<WorkerInfo> workingWorkers;

	private final int timeOut;
	private final int sleepTime;

	public MasterHeartBeat(int pollingPort, int timeOut, int sleepTime) {
		this.pollingPort = pollingPort;
		this.timeOut = timeOut;
		this.sleepTime = sleepTime;
	}

	@Override
	public void run() {
		for (WorkerInfo worker : workers) {
			ExecutorService executor = Executors.newSingleThreadExecutor();
			Future<String> future = executor.submit(new PollingThread(worker));

			try {
				future.get(timeOut, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				// Ignores it
			} catch (ExecutionException e) {
				e.printStackTrace();
			} catch (TimeoutException e) {
				if (!failedWorkers.contains(worker)) {
					failedWorkers.add(worker);
				}
				workingWorkers.remove(worker);
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
			Socket poll = new Socket(worker.getIpAddress(), worker.getPort());
			PrintWriter writer = new PrintWriter(poll.getOutputStream());
			Scanner s = new Scanner(poll.getInputStream());
			writer.println("Still Working?");
			String response = s.nextLine();
			s.close();
			poll.close();
			return response;
		}

	}
}