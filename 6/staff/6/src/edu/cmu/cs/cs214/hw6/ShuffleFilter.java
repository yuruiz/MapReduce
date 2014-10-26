package edu.cmu.cs.cs214.hw6;

import java.io.Serializable;

/**
 * Used by the {@link ShuffleWorkerCommand} to determine which intermediate
 * key/value pairs should be sent back to the reduce worker during the shuffle.
 */
public class ShuffleFilter implements Serializable {
    private static final long serialVersionUID = -4917053023291472880L;

    private final int mId;
    private final int mNumReducers;

    public ShuffleFilter(int id, int numReducers) {
        mId = id;
        mNumReducers = numReducers;
    }

    public boolean isValidKey(String key) {
        return Math.abs(key.hashCode() % mNumReducers) == mId;
    }

}
