package nl.esciencecenter.newsreader.hbase;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

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

    @Parameter(names="--cache", description="Number of rows to cache")
    private int cacheSize = 10;

    public void run() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = HBaseConfiguration.create();
        config.set("familyName", familyName);
        config.set("columnName", columnName);
        Job job = Job.getInstance(config);
        job.setJarByClass(Sizer.class);     // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(cacheSize);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        scan.addColumn(familyName.getBytes(), columnName.getBytes());

        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.initTableMapperJob(
                tableName,        // input HBase table name
                scan,             // Scan instance to control CF and attribute selection
                SizerMapper.class,   // mapper
                Text.class,
                IntWritable.class,
                job);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setNumReduceTasks(0);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}