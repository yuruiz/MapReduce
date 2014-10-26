package edu.cmu.cs.cs214.hw6;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.cmu.cs.cs214.hw6.util.CollectionUtils;
import edu.cmu.cs.cs214.hw6.util.Log;
import edu.cmu.cs.cs214.hw6.util.StaffUtils;

/**
 * This class represents the "master server" in the distributed map/reduce
 * framework. The {@link MasterServer} is in charge of managing the entire
 * map/reduce computation from beginning to end. The {@link MasterServer}
 * listens for incoming client connections on a distinct host/port address, and
 * is passed an array of {@link WorkerInfo} objects when it is first initialized
 * that provides it with necessary information about each of the available
 * workers in the system (i.e. each worker's name, host address, port number,
 * and the set of {@link Partition}s it stores). A single map/reduce computation
 * managed by the {@link MasterServer} will typically behave as follows:
 *
 * <ol>
 * <li>Wait for the client to submit a map/reduce task.</li>
 * <li>Distribute the {@link MapTask} across a set of "map-workers" and wait for
 * all map-workers to complete.</li>
 * <li>Distribute the {@link ReduceTask} across a set of "reduce-workers" and
 * wait for all reduce-workers to complete.</li>
 * <li>Write the locations of the final results files back to the client.</li>
 * </ol>
 */
public class MasterServer extends Thread {
    private static final String TAG = MasterServer.class.getSimpleName();

    private final int mPort;
    private final ExecutorService mServerExecutor;
    private final ExecutorService mClientTaskExecutor;
    private final List<WorkerInfo> mWorkers;

    /**
     * The {@link MasterServer} constructor.
     *
     * @param masterPort The port to listen on.
     * @param workers Information about each of the available workers in the
     *        system.
     */
    public MasterServer(int masterPort, List<WorkerInfo> workers) {
        mPort = masterPort;
        mServerExecutor = Executors.newSingleThreadExecutor();
        mClientTaskExecutor = Executors.newFixedThreadPool(Runtime.getRuntime()
                .availableProcessors());
        mWorkers = workers;
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

            Log.i(TAG, "Waiting for incoming client connections on port " + mPort + ".");

            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    mServerExecutor.execute(new ClientTaskHandler(clientSocket, mWorkers,
                            mClientTaskExecutor));
                } catch (IOException e) {
                    Log.e(TAG, "Error while listening for incoming connections.", e);
                    break;
                }
            }

            Log.i(TAG, "Shutting down...");

            try {
                serverSocket.close();
            } catch (IOException e) {
            }
        } finally {
            mServerExecutor.shutdown();
            mClientTaskExecutor.shutdown();
        }
    }

    private static class ClientTaskHandler implements Runnable {
        private final Socket mSocket;
        private final ExecutorService mExecutor;
        private final List<MapReduceResult> mResults;
        private final List<WorkerInfo> mWorkers;

        public ClientTaskHandler(Socket socket, List<WorkerInfo> workers, ExecutorService executor) {
            mSocket = socket;
            mExecutor = executor;
            mResults = Collections.synchronizedList(new ArrayList<MapReduceResult>());
            mWorkers = workers;
        }

        @Override
        public void run() {
            try {
                ObjectInputStream in = null;
                try {
                    in = new ObjectInputStream(mSocket.getInputStream());
                } catch (IOException e) {
                    Log.e(TAG, "Could not create input/output stream to client.", e);
                    return;
                }

                // (1) Read the MapTask and ReduceTask from the client.
                MapTask mapTask = null;
                ReduceTask reduceTask = null;

                try {
                    mapTask = (MapTask) in.readObject();
                    reduceTask = (ReduceTask) in.readObject();
                } catch (IOException e) {
                    Log.e(TAG, "Failed to read tasks from the client.", e);
                    return;
                } catch (ClassNotFoundException e) {
                    Log.e(TAG, "Received invalid map/reduce task from client.", e);
                    return;
                }

                Log.i(TAG, "Received map/reduce tasks from client.");

                Map<WorkerInfo, List<String>> workerPartitions = createWorkerPartitionMap();

                List<MapCallable> mapCallables = new ArrayList<>();
                for (Map.Entry<WorkerInfo, List<String>> entry : workerPartitions.entrySet()) {
                    WorkerInfo worker = entry.getKey();
                    Set<String> partitions = new HashSet<>(entry.getValue());
                    mapCallables.add(new MapCallable(mapTask, worker, partitions));
                }

                Log.i(TAG, "Created map/reduce callables.");

                Set<WorkerInfo> availableWorkers = new HashSet<>(mWorkers);
                Set<String> allFailedPartitions = new HashSet<>();
                try {
                    // (2) Map phase.
                    while (true) {
                        boolean retry = false;
                        allFailedPartitions.clear();

                        // Initiate the map phase and wait for it to complete.
                        List<Future<Void>> mapResults = mExecutor.invokeAll(mapCallables);
                        for (int f = 0; f < mapResults.size(); f++) {
                            try {
                                mapResults.get(f).get();
                                Log.i(TAG, "Map callable success.");
                            } catch (ExecutionException e) {
                                Log.e(TAG, "Map callable failure.");
                                MapCallable callable = mapCallables.get(f);
                                WorkerInfo failedWorker = callable.getWorker();
                                workerPartitions.remove(failedWorker);
                                availableWorkers.remove(failedWorker);
                                allFailedPartitions.addAll(callable.getPartitions());
                                retry = true;
                            }
                        }

                        if (!retry) {
                            break;
                        }

                        mapCallables.clear();
                        for (String failedPartition : allFailedPartitions) {
                            boolean foundWorker = false;
                            for (WorkerInfo w : availableWorkers) {
                                Set<String> partitionNames = new HashSet<>();
                                for (Partition p : w.getPartitions()) {
                                    partitionNames.add(p.getPartitionName());
                                }
                                if (partitionNames.contains(failedPartition)) {
                                    Set<String> partitions = new HashSet<>();
                                    partitions.add(failedPartition);
                                    mapCallables.add(new MapCallable(mapTask, w, partitions));
                                    CollectionUtils.putIfAbsent(workerPartitions, w,
                                            failedPartition);
                                    foundWorker = true;
                                    break;
                                }
                            }
                            if (!foundWorker) {
                                Log.e(TAG, "Failed to find worker for partition " + failedPartition);
                                return;
                            }
                        }
                    }

                    Log.i(TAG, "Map phase complete!");

                    // (3) Reduce phase.
                    int i = 0;
                    List<ReduceCallable> reduceCallables = new ArrayList<>();
                    for (WorkerInfo worker : availableWorkers) {
                        reduceCallables.add(new ReduceCallable(i, reduceTask, worker,
                                workerPartitions, mResults));
                        i++;
                    }

                    // Initiate the reduce phase and wait for it to complete.
                    List<Future<Void>> reduceResults = mExecutor.invokeAll(reduceCallables);
                    for (int f = 0; f < reduceResults.size(); f++) {
                        try {
                            reduceResults.get(f).get();
                            Log.i(TAG, "Reduce callable success.");
                        } catch (ExecutionException e) {
                            Log.e(TAG, "Reduce callable failure.");

                            // TODO: handling reduce worker failures has not yet
                            // been implemented!
                        }
                    }

                    Log.i(TAG, "Reduce phase complete!");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } finally {
                // (4) Report the results back to the client.
                try {
                    ObjectOutputStream out = new ObjectOutputStream(mSocket.getOutputStream());
                    out.writeObject(mResults);
                } catch (IOException e) {
                    Log.e(TAG, "Failed to write final results back to the client.", e);
                } finally {
                    try {
                        mSocket.close();
                    } catch (IOException e) {
                        // Ignore because we're about to exit anyway.
                    }
                }
            }
        }

        private Map<WorkerInfo, List<String>> createWorkerPartitionMap() {
            Map<String, List<WorkerInfo>> partitionMap = new HashMap<>();
            for (WorkerInfo w : mWorkers) {
                for (Partition p : w.getPartitions()) {
                    CollectionUtils.putIfAbsent(partitionMap, p.getPartitionName(), w);
                }
            }
            Map<WorkerInfo, List<String>> workerPartitions = new HashMap<>();
            for (Map.Entry<String, List<WorkerInfo>> entry : partitionMap.entrySet()) {
                List<WorkerInfo> partitionWorkers = entry.getValue();
                int rand = (int) (Math.random() * partitionWorkers.size());
                CollectionUtils.putIfAbsent(workerPartitions, partitionWorkers.get(rand),
                        entry.getKey());
            }
            return workerPartitions;
        }
    }

    /********************************************************************/
    /***************** STAFF CODE BELOW. DO NOT MODIFY. *****************/
    /********************************************************************/

    /**
     * Starts the master server on a distinct port. Information about each
     * available worker in the distributed system is parsed and passed as an
     * argument to the {@link MasterServer} constructor. This information can be
     * either specified via command line arguments or via system properties
     * specified in the <code>master.properties</code> and
     * <code>workers.properties</code> file (if no command line arguments are
     * specified).
     */
    public static void main(String[] args) {
        StaffUtils.makeMasterServer(args).start();
    }

}
