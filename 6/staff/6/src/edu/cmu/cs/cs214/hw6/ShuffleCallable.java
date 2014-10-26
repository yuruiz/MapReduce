package edu.cmu.cs.cs214.hw6;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import edu.cmu.cs.cs214.hw6.util.CollectionUtils;
import edu.cmu.cs.cs214.hw6.util.Log;

/**
 * Instances of this class are instantiated and invoked by the
 * {@link ReduceWorkerCommand} during the shuffle. When invoked, it will
 * establish a socket connection with the map worker, will write a
 * {@link ShuffleWorkerCommand} to the remote map worker server to be executed,
 * and will wait for the {@link ShuffleWorkerCommand} to send it intermediate
 * key/value pair results generated during the map phase.
 */
public class ShuffleCallable implements Callable<Void> {
    private static final String TAG = ShuffleCallable.class.getSimpleName();

    private final WorkerInfo mMapWorker;
    private final List<String> mPartitions;
    private final Map<String, List<String>> mResults;
    private final ShuffleFilter mFilter;

    public ShuffleCallable(WorkerInfo mapper, List<String> partitions,
            Map<String, List<String>> results, ShuffleFilter filter) {
        mMapWorker = mapper;
        mPartitions = partitions;
        mResults = results;
        mFilter = filter;
    }

    @Override
    public Void call() throws Exception {
        try (Socket socket = new Socket(mMapWorker.getHost(), mMapWorker.getPort())) {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(new ShuffleWorkerCommand(mFilter, mMapWorker, mPartitions));
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            while (true) {
                String key = (String) in.readObject();
                if (key == null) {
                    break;
                }
                String value = (String) in.readObject();
                synchronized (mResults) {
                    CollectionUtils.putIfAbsent(mResults, key, value);
                }
            }
        } catch (Exception e) {
            Log.e(TAG, String.format("Exception while interacting with map worker: %s.",
                    mMapWorker.getName()), e);
            throw e;
        }
        return null;
    }

}
