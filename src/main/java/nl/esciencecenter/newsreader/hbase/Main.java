package nl.esciencecenter.newsreader.hbase;

import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * Main class
 */
public class Main {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        BasicConfigurator.configure();
        Commander commander = new Commander();
        commander.main(args);
    }
}
