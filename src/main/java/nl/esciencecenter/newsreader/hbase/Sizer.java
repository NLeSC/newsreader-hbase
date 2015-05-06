package nl.esciencecenter.newsreader.hbase;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.hbase.HBaseScheme;
import cascading.hbase.HBaseTap;
import cascading.hbase.helper.HBaseMapToTuples;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.WritableSequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
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

        Fields hbaseFields = new Fields("docName", "cf", "column", columnName);
        Fields contentArgs = new Fields("docName", columnName);
        Fields sizeArgs = new Fields("docName", "docSize");

        Pipe pipe = new Pipe("sizeOfDoc");
        pipe = new Each(pipe, contentArgs, new HBaseMapToTuples(hbaseFields, contentArgs));
        pipe = new Each(pipe, contentArgs, new SizerFunction(contentArgs), Fields.REPLACE);

        WritableSequenceFile outseq = new WritableSequenceFile(sizeArgs, Text.class, IntWritable.class);
        Tap sink = new Hfs(outseq, outputPath);

        FlowDef flow = FlowDef.flowDef()
                .addSource( pipe, source )
                .addTailSink( pipe, sink );

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Sizer.class);
        FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);
        flowConnector.connect( flow ).complete();
    }

}