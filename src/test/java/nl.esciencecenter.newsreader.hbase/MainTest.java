package nl.esciencecenter.newsreader.hbase;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MainTest {

    private Main main;
    private DirectoryLoader loader;
    private DirectoryDumper dumper;

    @Before
    public void setUp() throws Exception {
        main = new Main();
        loader = mock(DirectoryLoader.class);
        main.setLoader(loader);
        dumper = mock(DirectoryDumper.class);
        main.setDumper(dumper);
    }

    @Test
    public void testRun_load() throws IOException {
        String[] args = {"load"};

        main.main(args);

        verify(loader).run();
    }

    @Test
    public void testRun_dump() throws IOException {
        String[] args = {"dump"};

        main.main(args);

        verify(dumper).run();
    }
}