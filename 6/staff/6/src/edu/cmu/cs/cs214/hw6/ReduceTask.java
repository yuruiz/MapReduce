package edu.cmu.cs.cs214.hw6;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

/**
 * A plug-in interface for the reduce portion of a map/reduce computation.
 *
 * DO NOT MODIFY THIS INTERFACE.
 */
public interface ReduceTask extends Serializable {

    /**
     * Given a key and an {@link Iterator} over all of the values for that key,
     * computes and outputs the final key/value pair result for the specified
     * key, and emitting the pair over the network using the provided
     * {@link Emitter}.
     *
     * @param key The key currently being reduced by this task.
     * @param values An {@link Iterator} over all of the values for this key.
     * @param out The {@link Emitter} to use to emit the final key/value pair
     *        result over the network back to the master.
     *
     * @throws IOException If an I/O error occurs.
     */
    void execute(String key, Iterator<String> values, Emitter emitter) throws IOException;

}
