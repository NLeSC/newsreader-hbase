package nl.esciencecenter.newsreader.hbase;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.hbase.HBaseScheme;
import cascading.hbase.HBaseTap;
import cascading.pipe.Each;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

@Parameters(separators="=", commandDescription="Map reduce job to get size of each document")
public class Sizer {

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

        Fields sizeArgs = new Fields("docName", "docSize");

        Each sizer = new Each("size", new SizerFunction(sizeArgs));

//        WritableSequenceFile outseq = new WritableSequenceFile(sizeArgs, Text.class, IntWritable.class);
        TextDelimited outseq = new TextDelimited(true, "\t");
        Tap sink = new Hfs(outseq, outputPath);

        // copy hbase config to cascading props
        Properties properties = new Properties();
        Configuration config = HBaseConfiguration.create();
        for (Map.Entry<String, String> tuple: config) {
            String key = tuple.getKey();
            properties.setProperty(key, tuple.getValue());
        }

        AppProps.setApplicationJarClass(properties, Sizer.class);
        FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);
        Flow flow = flowConnector.connect(source, sink, sizer);
        flow.writeDOT("flow.dot");
        flow.complete();
    }

}