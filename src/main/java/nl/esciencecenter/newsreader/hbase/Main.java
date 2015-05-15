package nl.esciencecenter.newsreader.hbase;

import java.io.IOException;
import org.apache.log4j.BasicConfigurator;

/**
 * Main class
 */
public class Main {
    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();
        Commander commander = new Commander();
        commander.main(args);
    }
}
