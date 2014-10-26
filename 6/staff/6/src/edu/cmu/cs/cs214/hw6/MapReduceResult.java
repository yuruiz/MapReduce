package edu.cmu.cs.cs214.hw6;

import java.io.File;
import java.io.Serializable;

/**
 * Represents a worker's final result of a map-reduce computation. The master
 * server should send a List of MapReduceResults back to the client so that the
 * client can know the location of the map-reduce computation's final results.
 * The client should then loop over the MapReduceResults, print each to standard
 * output.
 */
public class MapReduceResult implements Serializable {
    private static final long serialVersionUID = 1689275643584705160L;

    private final File mFile;
    private final String mWorkerHost;
    private final int mWorkerPort;

    public MapReduceResult(File file, String workerHost, int workerPort) {
        mFile = file;
        mWorkerHost = workerHost;
        mWorkerPort = workerPort;
    }

    public File getFile() {
        return mFile;
    }

    public String getWorkerHost() {
        return mWorkerHost;
    }

    public int getWorkerPort() {
        return mWorkerPort;
    }

    @Override
    public String toString() {
        return String.format("<%s: file=%s, workerHost=%s, workerPort=%d>",
                MapReduceResult.class.getSimpleName(), mFile.toString(), mWorkerHost, mWorkerPort);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (mFile == null ? 0 : mFile.hashCode());
        result = prime * result + (mWorkerHost == null ? 0 : mWorkerHost.hashCode());
        result = prime * result + mWorkerPort;
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MapReduceResult)) {
            return false;
        }
        MapReduceResult other = (MapReduceResult) o;
        return equals(mFile, other.mFile) && equals(mWorkerHost, other.mWorkerHost)
                && mWorkerPort == other.mWorkerPort;
    }

    private static boolean equals(Object o1, Object o2) {
        return o1 == null ? o2 == null : o1.equals(o2);
    }

}
