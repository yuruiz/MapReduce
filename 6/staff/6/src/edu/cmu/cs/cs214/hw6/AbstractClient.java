package edu.cmu.cs.cs214.hw6;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;

import edu.cmu.cs.cs214.hw6.plugin.wordcount.WordCountClient;
import edu.cmu.cs.cs214.hw6.plugin.wordprefix.WordPrefixClient;
import edu.cmu.cs.cs214.hw6.util.Log;

/**
 * An abstract client class used primarily for code reuse between the
 * {@link WordCountClient} and {@link WordPrefixClient}.
 *
 * Complete the implementation of the {@link #execute()} method in this class.
 * The {@link #execute()} method should simply submit the {@link MapTask} and
 * {@link ReduceTask} to the {@link MasterServer} and wait for it to respond
 * with the final results.
 */
public abstract class AbstractClient {
    private static final String TAG = AbstractClient.class.getSimpleName();

    private final String mMasterHost;
    private final int mMasterPort;

    /**
     * The {@link AbstractClient} constructor.
     *
     * @param masterHost The host name of the {@link MasterServer}.
     * @param masterPort The port that the {@link MasterServer} is listening on.
     */
    public AbstractClient(String masterHost, int masterPort) {
        mMasterHost = masterHost;
        mMasterPort = masterPort;
    }

    protected abstract MapTask getMapTask();

    protected abstract ReduceTask getReduceTask();

    // Necessary because we are casting Object to generic List<MapReduceResult>.
    @SuppressWarnings("unchecked")
    public void execute() {
        long start = System.currentTimeMillis();

        final MapTask mapTask = getMapTask();
        final ReduceTask reduceTask = getReduceTask();

        try (Socket socket = new Socket(mMasterHost, mMasterPort)) {
            // (1) Send the map task and reduce task to the master server.
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(mapTask);
            out.writeObject(reduceTask);

            // (2) Wait for the master server to send back the results.
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            List<MapReduceResult> results = (List<MapReduceResult>) in.readObject();
            for (MapReduceResult result : results) {
                System.out.println(result);
            }
        } catch (IOException e) {
            Log.e(TAG, "I/O Exception while interacting with master server.", e);
        } catch (ClassNotFoundException e) {
            Log.e(TAG, "Received invalid result from master server.", e);
        }

        long end = System.currentTimeMillis();
        System.out.println("\nMap/Reduce computation took: " + (end - start) + " ms.");
    }

}
