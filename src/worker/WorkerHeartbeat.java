package worker;

import util.Constant;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

public class WorkerHeartbeat extends Thread {

	private Worker worker;

	public WorkerHeartbeat(Worker worker) {
		this.worker = worker;
	}

	@Override
	public void run() {

		System.out.println("Heart beat start");

		/*Create the listen socket to receive the heart beat request from master node*/
		ServerSocket serverSocket = null;
		try {
			System.out.println("Open server at: "
					+ worker.getInfo().getPollingPort());
			serverSocket = new ServerSocket(worker.getInfo().getPollingPort());
		} catch (IOException e) {
			e.printStackTrace();
		}

		assert serverSocket != null;

		try {
			/*Processing the Heart beat request*/
			while (true) {
				Socket socket = serverSocket.accept();
				Scanner s = new Scanner(socket.getInputStream());

				String response = s.nextLine();
				PrintWriter writer = new PrintWriter(socket.getOutputStream(),
						true);

				if (response.equals(Constant.HEART_BEAT_QUERY)) {
					writer.println(Constant.HEART_BEAT_RESPONSE);
				}

				s.close();
				writer.close();
				socket.close();
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}