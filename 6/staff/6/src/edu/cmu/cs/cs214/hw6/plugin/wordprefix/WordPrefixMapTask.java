package edu.cmu.cs.cs214.hw6.plugin.wordprefix;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import edu.cmu.cs.cs214.hw6.Emitter;
import edu.cmu.cs.cs214.hw6.MapTask;

/**
 * Sample word prefix map task implementation (for course staff use only).
 */
public class WordPrefixMapTask implements MapTask {
    private static final long serialVersionUID = 3046495241158633404L;

    @Override
    public void execute(InputStream in, Emitter emitter) throws IOException {
        Scanner scanner = new Scanner(in);
        scanner.useDelimiter("\\W+");
        while (scanner.hasNext()) {
            String word = scanner.next().trim().toLowerCase();
            // Emit each prefix of the word, including the word itself.
            for (int i = 0; i < word.length(); i++) {
                String prefix = word.substring(0, i + 1);
                emitter.emit(prefix, word);
            }
        }
        scanner.close();
    }

}
