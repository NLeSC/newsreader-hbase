package nl.esciencecenter.newsreader.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

public class HTableHelperTest {

    private Configuration config;
    private HBaseAdmin admin;

    @Before
    public void setUp() {
        config = mock(Configuration.class);
        admin = mock(HBaseAdmin.class);
        HTableHelper.setAdmin(admin);
    }
    
    @Test
    public void testCreateTable_TableExists() throws Exception {
        TableName tname = TableName.valueOf("documents");
        when(admin.tableExists(tname)).thenReturn(true);

        HTableHelper.createTable("documents", "naf", config);

        verify(admin, never()).createTable(Mockito.<HTableDescriptor>anyObject());
    }
}