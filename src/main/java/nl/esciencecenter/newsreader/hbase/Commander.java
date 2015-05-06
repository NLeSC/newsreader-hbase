package nl.esciencecenter.newsreader.hbase;

import com.beust.jcommander.JCommander;

import java.io.IOException;

/**
 * Command line parser and runs sub-commands
 */
public class Commander {
    private final Sizer sizer;
    private DirectoryLoader loader;
    private DirectoryDumper dumper;

    public Commander() {
        loader = new DirectoryLoader();
        dumper = new DirectoryDumper();
        sizer = new Sizer();
    }

    public Commander(DirectoryLoader loader, DirectoryDumper dumper, Sizer sizer) {
        this.loader = loader;
        this.dumper = dumper;
        this.sizer = sizer;
    }

    public void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        JCommander jc = new JCommander();
        jc.addCommand("load", loader);
        jc.addCommand("dump", dumper);
        jc.addCommand("sizer", sizer);

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
                case "sizer":
                    sizer.run();
                default:
                    jc.usage();
            }
        }
    }
}
