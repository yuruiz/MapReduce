package edu.cmu.cs.cs214.hw6;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * An {@link Emitter} that emits key/value pairs to a file on the disk.
 */
public class FileEmitter implements Emitter {
    private final PrintWriter mWriter;

    public FileEmitter(File file) throws FileNotFoundException {
        mWriter = new PrintWriter(new FileOutputStream(file), true);
    }

    @Override
    public void emit(String key, String value) throws IOException {
        mWriter.println(key + " " + value);
    }

    @Override
    public void close() {
        mWriter.close();
    }

}
