package edu.cmu.cs.cs214.hw6.plugin.wordprefix;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.cmu.cs.cs214.hw6.Emitter;
import edu.cmu.cs.cs214.hw6.ReduceTask;

/**
 * Sample word prefix reduce task implementation (for course staff use only).
 */
public class WordPrefixReduceTask implements ReduceTask {
    private static final long serialVersionUID = 6763871961687287020L;

    @Override
    public void execute(String key, Iterator<String> values, Emitter emitter)
            throws IOException {
        Map<String, Integer> counts = new HashMap<>();

        while (values.hasNext()) {
            String completion = values.next();
            if (counts.containsKey(completion)) {
                counts.put(completion, counts.get(completion) + 1);
            } else {
                counts.put(completion, 1);
            }
        }

        Map.Entry<String, Integer> maxEntry = null;
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            if (maxEntry == null || entry.getValue() > maxEntry.getValue()) {
                maxEntry = entry;
            }
        }

        if (maxEntry == null) {
            emitter.emit(key, key);
        } else {
            emitter.emit(key, maxEntry.getKey());
        }
    }

}
