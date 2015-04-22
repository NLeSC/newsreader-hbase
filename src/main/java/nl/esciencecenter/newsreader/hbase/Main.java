package nl.esciencecenter.newsreader.hbase;

import com.beust.jcommander.JCommander;

import java.io.IOException;

public class Main {
    private static DirectoryLoader loader;
    private static DirectoryDumper dumper;

    public Main() {
        loader = new DirectoryLoader();
        dumper = new DirectoryDumper();
    }

    public static void setLoader(DirectoryLoader loader) {
        Main.loader = loader;
    }

    public static void setDumper(DirectoryDumper dumper) {
        Main.dumper = dumper;
    }

    public static void main(String[] args) throws IOException {
        JCommander jc = new JCommander();
        jc.addCommand("load", loader);
        jc.addCommand("dump", dumper);

        jc.parse(args);

        String command = jc.getParsedCommand();
        if (command == null) {
            jc.usage();
        } else {
            switch (command) {
                case "load":
                    loader.run();
                    break;
                case "dump":
                    dumper.run();
                    break;
                default:
                    jc.usage();
            }
        }
    }
}
