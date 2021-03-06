package nl.esciencecenter.newsreader.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class HTableHelper {
	private static final Logger LOG = LoggerFactory.getLogger(DirectoryLoaderAction.class);
	
    private static HBaseAdmin admin;

    public static void setAdmin(HBaseAdmin admin) {
        HTableHelper.admin = admin;
    }

    /**
     * Creates table if it does not exist.
     *
     * @param tableName Name of table
     * @param familyName Name of family
     * @param config HBase config
     * @throws IOException When unable to connect to HBase
     */
    public static void createTable(String tableName, String familyName, Configuration config) throws IOException {
        if (admin == null) {
            admin = new HBaseAdmin(config);
        }
        TableName tname = TableName.valueOf(tableName);
        if (!admin.tableExists(tname)) {
            LOG.debug("Creating table {}", tname);
            HTableDescriptor tableDesc = new HTableDescriptor(tname);
            HColumnDescriptor columnDesc = new HColumnDescriptor(familyName.getBytes());
            // to have compression, hadoop native shared libraries are required
            // See http://hbase.apache.org/book.html#_compressor_configuration_installation_and_use
            // input 1945 files of 2.1Gb,
            // LZ4 = 2.8Gb datadir, 43s
            // GZ = 2.6Gb datadir, 1m12s with freezes of 1 second
            // None = 7.2Gb, 1m1

            // disable compression it is not working yet on SURFSara hbase cluster
//            columnDesc.setCompressionType(Compression.Algorithm.LZ4);

            // async_wall: without 57s, with 1m5s
            // lz4 + async = 42s
            // lz4 + sync = 42s
            // tableDesc.setDurability(Durability.ASYNC_WAL);
            tableDesc.addFamily(columnDesc);
            admin.createTable(tableDesc);
        }
    }

    /**
     * Retrieve a table object
     *
     * @param tableName Name of table
     * @param familyName Name of family
     * @param config HBase config
     * @return A HTable which can be used for puts, get, scans, etc.
     * @throws IOException When unable to connect to HBase
     */
    public static HTable getTable(String tableName, String familyName, Configuration config) throws IOException {
        createTable(tableName, familyName, config);
        HTable table = new HTable(config, tableName);
        // disable auto flush so puts are batched
        // and flushed when write buffer size is reached
        table.setAutoFlush(false, false);
        return table;
    }
}
