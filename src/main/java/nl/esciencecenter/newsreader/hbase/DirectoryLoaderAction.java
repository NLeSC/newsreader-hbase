package nl.esciencecenter.newsreader.hbase;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.PrivilegedAction;


public class DirectoryLoaderAction implements PrivilegedAction<Long> {
	private static final Logger LOG = LoggerFactory.getLogger(DirectoryLoaderAction.class);
	
    private String rootDirectory;
    private String tableName = "documents";
    private String familyName = "naf";
    private String columnName = "annotated";
    private long writeBufferSize = 100 * 1024 * 1024;

    private HTable table;
    private long addedDocumentCounter = 0;
    private Configuration config;

    public DirectoryLoaderAction(String rootDirectory, String tableName, String familyName, String columnName, long writeBufferSize, Configuration config) {
        this.rootDirectory = rootDirectory;
        this.tableName = tableName;
        this.familyName = familyName;
        this.columnName = columnName;
        this.writeBufferSize = writeBufferSize;
        this.config = config;
    }

    public DirectoryLoaderAction(Configuration config) {
        this.config = config;
    }

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
            table = HTableHelper.getTable(tableName, familyName, config);
            // default write buffer size is 2097152, but a annotated documents are >1Mb,
            // so a flush/commit will be done each document
            // increase it so several documents are flushed
        }
        table.setWriteBufferSize(writeBufferSize);
    }

    @Override
    public Long run() {
        try {
            setup();

            Path rootPath = Paths.get(rootDirectory);
            walkDirectory(rootPath);

            cleanup();
        } catch (IOException e) {
            System.err.println(e);
        }

        return addedDocumentCounter;
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

        LOG.info("Loading directory: {}", rootDirectory);
        Files.walkFileTree(rootPath, visitor);
    }

    private void addDocument(Path file) {
        String filename = file.getFileName().toString().toLowerCase();
        if (filename.endsWith(".bz2")) {
            try {
                InputStream in = Files.newInputStream(file);
                BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(in);
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                int n = 0;
                byte[] buffer = new byte[1024];
                while (-1 != (n = bzIn.read(buffer))) {
                    bos.write(buffer, 0, n);
                }
                bzIn.close();
                byte[] content = bos.toByteArray();
                addDocumentContent(filename, content);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (filename.endsWith(".naf")) {
            try {
                byte[] content = Files.readAllBytes(file);
                addDocumentContent(filename, content);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void addDocumentContent(String filename, byte[] content) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        LOG.trace("Inserting = {}", filename);
        Put put = new Put(filename.getBytes());
        put.add(familyName.getBytes(), columnName.getBytes(), content);
        table.put(put);
        addedDocumentCounter ++;
    }
}
