package edu.cmu.cs.cs214.hw6;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Scanner;

import edu.cmu.cs.cs214.hw6.util.Log;
import edu.cmu.cs.cs214.hw6.util.WorkerStorage;

/**
 * Written to a map worker server by a {@link ShuffleCallable} during the
 * shuffle.
 */
public class ShuffleWorkerCommand extends WorkerCommand {
    private static final String TAG = ShuffleWorkerCommand.class.getSimpleName();
    private static final long serialVersionUID = 4805888045690606430L;

    private final ShuffleFilter mFilter;
    private final WorkerInfo mWorkerInfo;
    private final List<String> mPartitions;

    public ShuffleWorkerCommand(ShuffleFilter filter, WorkerInfo info, List<String> partitions) {
        mFilter = filter;
        mWorkerInfo = info;
        mPartitions = partitions;
    }

    @Override
    public void run() {
        try {
            ObjectOutputStream out = new ObjectOutputStream(getSocket().getOutputStream());
            for (String partitionName : mPartitions) {
                File mapResultsFile = new File(
                        WorkerStorage.getIntermediateResultsDirectory(mWorkerInfo.getName()),
                        String.format(WorkerStorage.INTERMEDIARY_RESULTS_FILE, partitionName));
                try (Scanner scanner = new Scanner(mapResultsFile)) {
                    while (scanner.hasNextLine()) {
                        String[] pair = scanner.nextLine().split(" ");
                        if (mFilter.isValidKey(pair[0])) {
                            out.writeObject(pair[0]);
                            out.writeObject(pair[1]);
                            out.reset();
                        }
                    }
                }
            }
            out.writeObject(null);
        } catch (IOException e) {
            Log.e(TAG, "Error transferring data from map worker to reduce worker.", e);
        }
    }
}
