package edu.cmu.cs.cs214.hw6;

import java.util.List;

import edu.cmu.cs.cs214.hw6.util.StaffUtils;

/**
 * Defines a generic worker server in the distributed system. Each
 * {@link WorkerServer} listens for incoming connections on a distinct host/port
 * address, and waits for others to send {@link WorkerCommand} objects for it to
 * execute remotely.
 *
 * Refer to recitation 13 for an idea of how this class should be implemented
 * (you are allowed to copy the code from recitation 13).
 */
public class WorkerServer extends Thread {
    private final int mPort;

    /**
     * The {@link WorkerServer} constructor.
     *
     * @param workerPort The port to listen on.
     */
    public WorkerServer(int workerPort) {
        mPort = workerPort;
    }

    @Override
    public void run() {
        // TODO: Implement this!
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
