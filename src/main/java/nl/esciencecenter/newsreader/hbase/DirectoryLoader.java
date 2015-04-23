package nl.esciencecenter.newsreader.hbase;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

@Parameters(separators="=", commandDescription="Load directory of naf files into HBase")
public class DirectoryLoader {

    @Parameter(names="--root", description="Root directory", required=true)
    private String rootDirectory;

    @Parameter(names="--table", description="HBase table name")
    private String tableName = "documents";

    @Parameter(names="--family", description="HBase column family name")
    private String familyName = "naf";

    @Parameter(names="--column", description="HBase column qualifier name")
    private String columnName = "annotated";

    private HTable table;

    public String getRootDirectory() {
        return rootDirectory;
    }

    public void setRootDirectory(String rootDirectory) {
        this.rootDirectory = rootDirectory;
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

    public void setTable(HTable table) {
        this.table = table;
    }

    private void setup() throws IOException {
        if (table == null) {
            Configuration config = HBaseConfiguration.create();
            table = HTableHelper.getTable(tableName, familyName, config);
        }
    }

    public void run() throws IOException {
        setup();

        Path rootPath = Paths.get(rootDirectory);
        walkDirectory(rootPath);

        cleanup();
    }

    private void cleanup() throws IOException {
        table.close();
    }

    private void walkDirectory(Path rootPath) throws IOException {
        FileVisitor<Path> visitor = new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                addDocument(file);
                return FileVisitResult.CONTINUE;
            }
        };

        System.out.println("Loading directory: " + rootDirectory);
        Files.walkFileTree(rootPath, visitor);
    }

    private void addDocument(Path file) {
        String filename = file.getFileName().toString().toLowerCase();
        if (filename.endsWith(".naf")) {
            System.out.println("Inserting = " + filename);
            try {
                byte[] content = Files.readAllBytes(file);
                Put put = new Put(filename.getBytes());
                put.add(familyName.getBytes(), columnName.getBytes(), content);
                table.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
