package nl.esciencecenter.newsreader.hbase;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlow;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.hbase.HBaseScheme;
import cascading.hbase.HBaseTap;
import cascading.pipe.Each;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

@Parameters(separators="=", commandDescription="Map reduce job to get size of each document")
public class Sizer {
	private static final Logger LOG = LoggerFactory.getLogger(Sizer.class);
	
    @Parameter(names="--table", description="HBase table name")
    private String tableName = "documents";

    @Parameter(names="--family", description="HBase column family name")
    private String familyName = "naf";

    @Parameter(names="--column", description="HBase column qualifier name")
    private String columnName = "annotated";

    @Parameter(names="--output", description="HDFS output path")
    private String outputPath = "/tmp/sizer.out";

    public void run() throws IOException, ClassNotFoundException, InterruptedException {
        Fields keyFields = new Fields( "docName" );
        String[] familyNames = {familyName};
        Fields[] valueFields = new Fields[]{new Fields(columnName)};
        HBaseScheme scheme = new HBaseScheme(keyFields, familyNames, valueFields);
        Tap source = new HBaseTap(tableName, scheme);
        
        // read from tab delimited file instead of hbase 
        // Tap source = new Hfs(new TextDelimited(true, "\t"), tableName);

        Fields sizeArgs = new Fields("docName", "docSize");
        Fields tableFields = new Fields("docName", columnName);
        Each sizer = new Each("size", tableFields, new SizerFunction(sizeArgs), Fields.RESULTS);
        
        TextDelimited outseq = new TextDelimited(true, "\t");
        Tap sink = new Hfs(outseq, outputPath);

        Properties properties = new Properties();

        AppProps.setApplicationJarClass(properties, Sizer.class);
        FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);
        HadoopFlow flow = (HadoopFlow) flowConnector.connect(source, sink, sizer);
        
        LOG.info("Obtaining HBASE token for hbase source tap");
        JobConf jobConf = flow.getFlowSteps().get(0).getConfig();
        TableMapReduceUtil.initCredentials(jobConf);
        
        flow.writeDOT("flow.dot");
        flow.complete();
    }

}