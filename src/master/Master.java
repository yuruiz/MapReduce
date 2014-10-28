package master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import worker.*;
import task.Job;
import util.Config;

public class Master {

	private Map<Integer, WorkerInfo> idToWorker;
	private List<WorkerInfo> workers;
	private List<Job> runningJobs;

	private Deque<Job> toBeDone;

	private final int port = Config.MASTER_PORT;

	public void run() {
		try {
			ServerSocket server = new ServerSocket(port);
			while (true) {
				Socket client = server.accept();
				ObjectOutputStream out = new ObjectOutputStream(
						client.getOutputStream());
				ObjectInputStream in = new ObjectInputStream(
						client.getInputStream());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static class ServiceHandler {
		public ServiceHandler() {
			
		}
	}

	public static void main() {

	}

}