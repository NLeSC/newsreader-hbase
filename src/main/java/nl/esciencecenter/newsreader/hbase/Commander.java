package nl.esciencecenter.newsreader.hbase;

import com.beust.jcommander.JCommander;

import java.io.IOException;

/**
 * Command line parser and runs sub-commands
 */
public class Commander {
    private DirectoryLoaderSubCommand loader;
    private DirectoryDumper dumper;

    public Commander() {
        loader = new DirectoryLoaderSubCommand();
        dumper = new DirectoryDumper();
    }

    public Commander(DirectoryLoaderSubCommand loader, DirectoryDumper dumper) {
        this.loader = loader;
        this.dumper = dumper;
    }

    public void main(String[] args) throws IOException {
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
