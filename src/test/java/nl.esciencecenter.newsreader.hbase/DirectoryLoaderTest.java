package nl.esciencecenter.newsreader.hbase;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DirectoryLoaderTest {
    private DirectoryLoader loader;
    private HTable table;

    @Before
    public void setUp() {
        loader = new DirectoryLoader();
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
        String expected = "/home/stefanv/data/newsreader/2000-medium-docs-en/docs/output/";
        assertThat(loader.getRootDirectory(), is(expected));
    }

    @Test
    public void testRun() throws Exception {
        // TODO don't walk actual test directory
        String rootDirectory = DirectoryLoaderTest.class.getClassLoader().getResource("naf-files").getPath();
        System.out.println(rootDirectory);
        loader.setRootDirectory(rootDirectory);

        loader.run();

        verify(table).put(Mockito.<Put>anyObject());
        verify(table).close();
    }
}