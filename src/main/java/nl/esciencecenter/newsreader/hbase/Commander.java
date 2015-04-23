package nl.esciencecenter.newsreader.hbase;

import com.beust.jcommander.JCommander;

import java.io.IOException;

public class Commander {
    private DirectoryLoader loader;
    private DirectoryDumper dumper;

    public Commander() {
        loader = new DirectoryLoader();
        dumper = new DirectoryDumper();
    }

    public Commander(DirectoryLoader loader, DirectoryDumper dumper) {
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
