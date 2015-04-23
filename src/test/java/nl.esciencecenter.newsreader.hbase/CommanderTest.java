package nl.esciencecenter.newsreader.hbase;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CommanderTest {

    private Commander commander;
    private DirectoryLoader loader;
    private DirectoryDumper dumper;

    @Before
    public void setUp() throws Exception {
        loader = mock(DirectoryLoader.class);
        dumper = mock(DirectoryDumper.class);
        commander = new Commander(loader, dumper);
    }

    @Test
    public void testRun_load() throws IOException {
        String[] args = {"load", "--root=/data"};

        commander.main(args);

        verify(loader).run();
    }

    @Test
    public void testRun_dump() throws IOException {
        String[] args = {"dump"};

        commander.main(args);

        verify(dumper).run();
    }
}