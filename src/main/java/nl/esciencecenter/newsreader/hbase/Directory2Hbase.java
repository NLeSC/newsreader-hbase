package nl.esciencecenter.newsreader.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;


public class Directory2Hbase {
    private static String tableName = "documents";
    private static String familyName = "naf";
    private static String columnName = "annotated";
    private static HTable table;

    public static void main(String[] args) throws IOException {
//        Path rootPath = Paths.get(args[0]);
        Path rootPath = Paths.get("/home/stefanv/data/newsreader/2000-medium-docs-en/docs/output/");
        System.out.println("Source " + rootPath.toString());

        initHBase();

        walkDirectory(rootPath);

        closeHBase();
    }

    private static void closeHBase() throws IOException {
        table.close();
    }

    private static void walkDirectory(Path rootPath) throws IOException {
        FileVisitor<Path> visitor = new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                addDocument(file);
                return FileVisitResult.CONTINUE;
            }
        };

        Files.walkFileTree(rootPath, visitor);
    }

    private static void initHBase() throws IOException {
        Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(config);
        TableName tname = TableName.valueOf(tableName);
        if (!admin.tableExists(tname)) {
            System.out.println("Creating table");
            HTableDescriptor tableDesc = new HTableDescriptor(tname);
            HColumnDescriptor columnDesc = new HColumnDescriptor(familyName.getBytes());
            // to have compression, hadoop native shared libraries are required
            // See http://hbase.apache.org/book.html#_compressor_configuration_installation_and_use
            columnDesc.setCompressionType(Compression.Algorithm.LZ4);
            tableDesc.addFamily(columnDesc);
            admin.createTable(tableDesc);
        }
        table = new HTable(config, tableName);
        table.setAutoFlush(false, false);
    }

    private static void addDocument(Path file) {
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