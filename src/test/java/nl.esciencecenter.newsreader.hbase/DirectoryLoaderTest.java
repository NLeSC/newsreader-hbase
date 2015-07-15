package nl.esciencecenter.newsreader.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DirectoryLoaderTest {
    private DirectoryLoaderAction loader;
    private HTable table;

    @Before
    public void setUp() {
        Configuration config = mock(Configuration.class);
        loader = new DirectoryLoaderAction(config);
        table = mock(HTable.class);
        loader.setTable(table);
    }

    @Test
    public void testDefaultTableName() {
        assertThat(loader.getTableName(), is("documents"));
    }

    @Test
    public void testDefaultFamilyName() {
        assertThat(loader.getFamilyName(), is("naf"));
    }

    @Test
    public void testDefaultColumnName() {
        assertThat(loader.getColumnName(), is("annotated"));
    }

    @Test
    public void testDefaultRootDirectory() {
        assertThat(loader.getRootDirectory(), is(nullValue()));
    }

    @Test
    public void testRunNaf() throws Exception {
        // TODO don't walk actual test directory
        String rootDirectory = DirectoryLoaderTest.class.getClassLoader().getResource("naf-files").getPath();
        System.out.println(rootDirectory);
        loader.setRootDirectory(rootDirectory);

        loader.run();

        verify(table).put(Mockito.<Put>anyObject());
        verify(table).close();
    }

    @Test
    public void testRunNafBz2() throws Exception {
        // TODO don't walk actual test directory
        String rootDirectory = DirectoryLoaderTest.class.getClassLoader().getResource("naf-files-bz2").getPath();
        System.out.println(rootDirectory);
        loader.setRootDirectory(rootDirectory);

        loader.run();

        verify(table).put(Mockito.<Put>anyObject());
        verify(table).close();
    }
}