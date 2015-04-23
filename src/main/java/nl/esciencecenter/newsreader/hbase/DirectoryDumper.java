package nl.esciencecenter.newsreader.hbase;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

@Parameters(separators="=", commandDescription="Dump list of rows in HBase column")
public class DirectoryDumper {

    @Parameter(names="--table", description="HBase table name")
    private String tableName = "documents";

    @Parameter(names="--family", description="HBase column family name")
    private String familyName = "naf";

    @Parameter(names="--column", description="HBase column qualifier name")
    private String columnName = "annotated";

    private HTable table;

    public DirectoryDumper() {
    }

    public void setTable(HTable table) {
        this.table = table;
    }

    private void setup() throws IOException {
        if (table == null) {
            Configuration config = HBaseConfiguration.create();
            table = HTableHelper.getTable(tableName, familyName, config);
        }
    }

    private void cleanup() throws IOException {
        table.close();
    }

    public void run() throws IOException {
        setup();

        scanTable();

        cleanup();
    }

    private void scanTable() throws IOException {
        Scan scan = new Scan();
        // 10Mb
        //scan.setMaxResultSize(10 * 1000 * 1000);
        scan.addColumn(familyName.getBytes(), columnName.getBytes());

        ResultScanner rs = table.getScanner(scan);
        try {
            for (Result r : rs) {
                String fileName = Bytes.toString(r.getRow());
                int fileSize = r.getValue(familyName.getBytes(), columnName.getBytes()).length;

                System.out.println(fileName + " : " + fileSize);
            }
        } finally {
            rs.close();
        }
    }

    public String getTableName() {
        return tableName;
    }

    public String getFamilyName() {
        return familyName;
    }

    public String getColumnName() {
        return columnName;
    }
}
