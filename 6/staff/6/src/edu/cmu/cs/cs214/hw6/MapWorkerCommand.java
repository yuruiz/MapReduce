package edu.cmu.cs.cs214.hw6;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Set;

import edu.cmu.cs.cs214.hw6.util.Log;
import edu.cmu.cs.cs214.hw6.util.WorkerStorage;

/**
 * Written to a map worker server by a {@link MapCallable} during the map phase.
 */
public class MapWorkerCommand extends WorkerCommand {
    private static final long serialVersionUID = 8067468961947958448L;
    private static final String TAG = MapWorkerCommand.class.getSimpleName();

    private final MapTask mTask;
    private final WorkerInfo mWorkerInfo;
    private final Set<String> mPartitions;

    public MapWorkerCommand(MapTask task, WorkerInfo info, Set<String> partitions) {
        mTask = task;
        mWorkerInfo = info;
        mPartitions = partitions;
    }

    @Override
    public void run() {
        try {
            for (String partitionName : mPartitions) {
                File intermediateFile = new File(
                        WorkerStorage.getIntermediateResultsDirectory(mWorkerInfo.getName()),
                        String.format(WorkerStorage.INTERMEDIARY_RESULTS_FILE, partitionName));
                try (Emitter emitter = new FileEmitter(intermediateFile)) {
                    Partition partition = new Partition(partitionName, mWorkerInfo.getName());
                    for (File file : partition) {
                        try (FileInputStream documentContents = new FileInputStream(file)) {
                            mTask.execute(documentContents, emitter);
                        } catch (FileNotFoundException e) {
                            Log.e(TAG, "File not found.", e);
                        } catch (IOException e) {
                            Log.e(TAG, "Could not close InputStream.", e);
                        }
                    }
                }
            }
            ObjectOutputStream out = new ObjectOutputStream(getSocket().getOutputStream());
            out.writeObject(null);
        } catch (IOException e) {
            Log.e(TAG, "Map worker received I/O exception while executing map task.", e);
        }
    }
}
