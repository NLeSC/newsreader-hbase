package nl.esciencecenter.newsreader.hbase;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * Loads a directory of naf files into HBase
 */
@Parameters(separators="=", commandDescription="Load directory of naf files into HBase")
public class DirectoryLoaderSubCommand {
	private static final Logger LOG = LoggerFactory.getLogger(DirectoryLoaderSubCommand.class);
	
    @Parameter(names="--root", description="Root directory", required=true)
    private String rootDirectory;

    @Parameter(names="--table", description="HBase table name")
    private String tableName = "documents";

    @Parameter(names="--family", description="HBase column family name")
    private String familyName = "naf";

    @Parameter(names="--column", description="HBase column qualifier name")
    private String columnName = "annotated";

    @Parameter(names="--buffer", description="Write buffer size in bytes")
    private long writeBufferSize = 100 * 1024 * 1024;

    public void run() {
        Configuration config = HBaseConfiguration.create();

        UserGroupInformation.setConfiguration(config);

        UserGroupInformation loginUser = null;
        try {
            loginUser = UserGroupInformation.getLoginUser();
        } catch (IOException e) {
            e.printStackTrace();
        }

        
        LOG.debug("Logged in as: {}", loginUser.getUserName());

        PrivilegedAction<Long> loader = new DirectoryLoaderAction(rootDirectory, tableName, familyName, columnName, writeBufferSize, config);
        long addedDocuments = loginUser.doAs(loader);

        LOG.info("{} documents added", addedDocuments);
    }
}
