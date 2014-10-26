package edu.cmu.cs.cs214.hw6;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.Callable;

import edu.cmu.cs.cs214.hw6.util.Log;

/**
 * Instances of this class are instantiated and invoked by the
 * {@link MasterServer} during the map phase. When invoked, it will establish a
 * socket connection with the map worker, will write a {@link MapWorkerCommand}
 * to the remote map worker server to be executed, and will wait until the
 * {@link MapWorkerCommand} has finished executing.
 */
public class MapCallable implements Callable<Void> {
    private static final String TAG = MapCallable.class.getSimpleName();

    private final MapTask mMapTask;
    private final WorkerInfo mWorker;
    private final Set<String> mPartitions;

    public MapCallable(MapTask task, WorkerInfo worker, Set<String> partitions) {
        mMapTask = task;
        mWorker = worker;
        mPartitions = partitions;
    }

    public WorkerInfo getWorker() {
        return mWorker;
    }

    public Set<String> getPartitions() {
        return mPartitions;
    }

    @Override
    public Void call() throws Exception {
        try (Socket socket = new Socket(mWorker.getHost(), mWorker.getPort())) {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(new MapWorkerCommand(mMapTask, mWorker, mPartitions));
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            in.readObject();
        } catch (IOException e) {
            Log.e(TAG, "I/O Exception while interacting with map worker.", e);
            throw e;
        } catch (ClassNotFoundException e) {
            Log.e(TAG, "Received invalid key/value pair from map worker.", e);
            throw e;
        }
        return null;
    }

}
