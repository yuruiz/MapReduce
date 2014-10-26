package edu.cmu.cs.cs214.hw6;

import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import edu.cmu.cs.cs214.hw6.util.Log;
import edu.cmu.cs.cs214.hw6.util.WorkerStorage;

/**
 * Instances of this class are instantiated and invoked by the
 * {@link MasterServer} during the reduce phase. When invoked, it will establish
 * a socket connection with the reduce worker, will write a
 * {@link ReduceWorkerCommand} to the remote reduce worker server to be
 * executed, and will wait for the {@link ReduceWorkerCommand} to finish
 * executing.
 */
public class ReduceCallable implements Callable<Void> {
    private static final String TAG = ReduceCallable.class.getSimpleName();

    private final int mId;
    private final ReduceTask mReduceTask;
    private final WorkerInfo mReduceWorker;
    private final Map<WorkerInfo, List<String>> mWorkerPartitions;
    private final List<MapReduceResult> mResults;

    public ReduceCallable(int id, ReduceTask task, WorkerInfo worker,
            Map<WorkerInfo, List<String>> workerPartitions, List<MapReduceResult> results) {
        mId = id;
        mReduceTask = task;
        mReduceWorker = worker;
        mWorkerPartitions = workerPartitions;
        mResults = results;
    }

    @Override
    public Void call() throws Exception {
        try (Socket socket = new Socket(mReduceWorker.getHost(), mReduceWorker.getPort())) {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ShuffleFilter filter = new ShuffleFilter(mId, mWorkerPartitions.keySet().size());
            File file = new File(WorkerStorage.getFinalResultsDirectory(mReduceWorker.getName()),
                    System.currentTimeMillis() + ".txt");
            out.writeObject(new ReduceWorkerCommand(filter, mReduceTask, mWorkerPartitions,
                    new MapReduceResult(file, mReduceWorker.getHost(), mReduceWorker.getPort())));
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            MapReduceResult result = (MapReduceResult) in.readObject();
            mResults.add(result);
        } catch (Exception e) {
            Log.e(TAG, String.format("Exception while interacting with reduce worker: %s.",
                    mReduceWorker.getName()), e);
            throw e;
        }
        return null;
    }
}
