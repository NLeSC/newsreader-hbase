package nl.esciencecenter.newsreader.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SizerMapper extends TableMapper<Text, IntWritable> {
    private byte[] familyName;
    private byte[] columnName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        familyName = Bytes.toBytes(context.getConfiguration().get("familyName"));
        columnName = Bytes.toBytes(context.getConfiguration().get("columnName"));
    }

    @Override
    public void map(ImmutableBytesWritable key, Result row, Context context) throws IOException, InterruptedException {
        byte[] ba = row.getValue(familyName, columnName);
        String content = Bytes.toString(ba);
        Text rowid = new Text(Bytes.toString(key.get()));
        // not all rows have the column filled
        if (content != null) {
            IntWritable length = new IntWritable(content.length());
            context.write(rowid, length);
        }
    }
}
