package edu.cmu.cs.cs214.hw6;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.cmu.cs.cs214.hw6.util.Log;

/**
 * Written to a reduce worker server by a {@link ReduceCallable} during the
 * reduce phase.
 */
public class ReduceWorkerCommand extends WorkerCommand implements Serializable {
    private static final long serialVersionUID = 1465877514912050967L;
    private static final String TAG = ReduceWorkerCommand.class.getSimpleName();

    private final ShuffleFilter mFilter;
    private final MapReduceResult mFinalResult;
    private final ReduceTask mTask;
    private final Map<WorkerInfo, List<String>> mWorkerPartitions;
    private final Map<String, List<String>> mResults;

    public ReduceWorkerCommand(ShuffleFilter filter, ReduceTask task,
            Map<WorkerInfo, List<String>> workerPartitions, MapReduceResult finalResult) {
        mFilter = filter;
        mFinalResult = finalResult;
        mTask = task;
        mWorkerPartitions = workerPartitions;
        mResults = new HashMap<>();
    }

    @Override
    public void run() {
        final Socket socket = getSocket();

        List<Callable<Void>> shuffleCallables = new ArrayList<>();
        for (Map.Entry<WorkerInfo, List<String>> entry : mWorkerPartitions.entrySet()) {
            // One shuffle callable for each map worker.
            shuffleCallables.add(new ShuffleCallable(entry.getKey(), entry.getValue(), mResults,
                    mFilter));
        }

        // ExecutorService can't be a private instance variable here since
        // it isn't Serializable.
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime()
                .availableProcessors());
        try {
            executor.invokeAll(shuffleCallables);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            File finalResultFile = mFinalResult.getFile();
            Emitter emitter = new FileEmitter(finalResultFile);
            for (Map.Entry<String, List<String>> entry : mResults.entrySet()) {
                mTask.execute(entry.getKey(), entry.getValue().iterator(), emitter);
            }
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(mFinalResult);
        } catch (IOException e) {
            Log.e(TAG, "I/O exception while performing reduce task.", e);
        }
    }
}
