package edu.cmu.cs.cs214.hw6;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

/**
 * A plug-in interface for the map portion of a map/reduce computation.
 *
 * DO NOT MODIFY THIS INTERFACE.
 */
public interface MapTask extends Serializable {

    /**
     * Executes the map portion of a map/reduce computation over the contents of
     * an input file, and emits the resulting key/value pairs to an output file
     * on the disk.
     *
     * @param in The {@link InputStream} to use to read the input data contents.
     * @param emitter The {@link Emitter} to use to write the intermediary
     *        key/value pair results.
     *
     * @throws IOException If an I/O error occurs.
     */
    void execute(InputStream in, Emitter emitter) throws IOException;

}
