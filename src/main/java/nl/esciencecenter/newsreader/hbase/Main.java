package nl.esciencecenter.newsreader.hbase;

import java.io.IOException;

/**
 * Main class
 */
public class Main {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Commander commander = new Commander();
        commander.main(args);
    }
}
