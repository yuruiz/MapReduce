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
import task.ClientJob;
import util.Config;

public class Master {

	private MasterHeartBeat hearBeat;
	private List<ClientJob> runningJobs;

	private Deque<ClientJob> toBeDone;

	private final int port = Config.MASTER_PORT;

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

	private class ServiceHandler implements Runnable {

		private ClientJob job;
		private Socket client;
		private ObjectOutputStream out;

		public ServiceHandler(ClientJob job, Socket client, ObjectOutputStream out) {
			this.job = job;
			this.client = client;
			this.out = out;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub

		}
	}
	
	

	public static void main() {

	}

}