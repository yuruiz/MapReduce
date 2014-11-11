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

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(worker.getInfo().getPollingPort());
        } catch (IOException e) {
            e.printStackTrace();
        }

        assert serverSocket != null;

        Socket socket;
        try {

            while ((socket = serverSocket.accept()) != null) {

                PrintWriter writer = new PrintWriter(socket.getOutputStream());

                Scanner s = new Scanner(socket.getInputStream());

                String response = s.nextLine();

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