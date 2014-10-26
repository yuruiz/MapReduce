package edu.cmu.cs.cs214.hw6;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.cmu.cs.cs214.hw6.util.Log;
import edu.cmu.cs.cs214.hw6.util.StaffUtils;

/**
 * Defines a generic worker server in the distributed system. Each
 * {@link WorkerServer} listens for incoming client connections on a distinct
 * host/port address, and waits for clients to send {@link WorkerCommand}
 * objects for it to execute remotely.
 *
 * @see WorkerCommand
 */
public class WorkerServer extends Thread {
    private static final String TAG = WorkerServer.class.getSimpleName();

    private final int mPort;
    private final ExecutorService mExecutor;

    /**
     * The {@link WorkerServer} constructor.
     *
     * @param workerPort The port to listen on.
     */
    public WorkerServer(int workerPort) {
        mPort = workerPort;
        mExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    @Override
    public void run() {
        try {
            ServerSocket serverSocket = null;
            try {
                serverSocket = new ServerSocket(mPort);
            } catch (IOException e) {
                Log.e(TAG, "Could not open server socket on port " + mPort + ".", e);
                return;
            }

            Log.i(TAG, "Listening for incoming commands on port " + mPort + ".");

            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    if(mPort == 15222)
                    	clientSocket.close();
                    mExecutor.execute(new WorkerCommandHandler(clientSocket));
                } catch (IOException e) {
                    Log.e(TAG, "Error while listening for incoming connections.", e);
                    break;
                }
            }

            Log.i(TAG, "Shutting down...");

            try {
                serverSocket.close();
            } catch (IOException e) {
                // Ignore because we're about to exit anyway.
            }
        } finally {
            mExecutor.shutdown();
        }
    }

    private static class WorkerCommandHandler implements Runnable {
        private final Socket mSocket;

        public WorkerCommandHandler(Socket socket) {
            mSocket = socket;
        }

        @Override
        public void run() {
            try (Socket socket = mSocket) {
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                WorkerCommand runnable = (WorkerCommand) in.readObject();
                runnable.setSocket(socket);
                runnable.run();
            } catch (IOException e) {
                Log.e(TAG, "I/O exception while interacting with client.", e);
            } catch (ClassNotFoundException e) {
                Log.e(TAG, "Received invalid task from client.", e);
            }
        }
    }

    /********************************************************************/
    /***************** STAFF CODE BELOW. DO NOT MODIFY. *****************/
    /********************************************************************/

    /**
     * Starts a worker server on a distinct port. This information can be either
     * specified as command line arguments or via system properties specified in
     * the <code>workers.properties</code> file (if no command line arguments
     * are specified).
     */
    public static void main(String[] args) {
        List<WorkerServer> servers = StaffUtils.makeWorkerServers(args);
        for (WorkerServer server : servers) {
            server.start();
        }
    }

}
