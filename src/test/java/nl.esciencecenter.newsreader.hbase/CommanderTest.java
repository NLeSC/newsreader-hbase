package nl.esciencecenter.newsreader.hbase;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CommanderTest {

    private Commander commander;
    private DirectoryLoaderSubCommand loader;
    private DirectoryDumper dumper;
    private Sizer sizer;

    @Before
    public void setUp() throws Exception {
        loader = mock(DirectoryLoaderSubCommand.class);
        dumper = mock(DirectoryDumper.class);
        sizer = mock(Sizer.class);
        commander = new Commander(loader, dumper, sizer);
    }

    @Test
    public void testRun_load() throws IOException, ClassNotFoundException, InterruptedException {
        String[] args = {"load", "--root=/data"};

        commander.main(args);

        verify(loader).run();
    }

    @Test
    public void testRun_dump() throws IOException, ClassNotFoundException, InterruptedException {
        String[] args = {"dump"};

        commander.main(args);

        verify(dumper).run();
    }

    @Test
    public void testRun_sizer() throws IOException, ClassNotFoundException, InterruptedException {
        String[] args = {"sizer"};

        commander.main(args);

        verify(sizer).run();
    }

}