package nl.esciencecenter.newsreader.hbase;

import com.google.common.collect.Iterators;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class DirectoryDumperTest {

    private DirectoryDumper dumper;
    private HTable table;
    private ResultScanner rs;

    @Before
    public void setUp() {
        dumper = new DirectoryDumper();
        table = mock(HTable.class);
        dumper.setTable(table);
    }

    public void setUpScan() throws IOException {
        rs = mock(ResultScanner.class);
        List<Cell> cells = new ArrayList<>();
        byte[] row = "5b1t-br51-jd3n-54fk.naf".getBytes();
        byte[] family = "naf".getBytes();
        byte[] column = "annotated".getBytes();
        byte type = KeyValue.Type.Maximum.getCode();
        long ts = new DateTime("2015-04-22T14:53:45.000Z").getMillis();
        byte[] value = "<some xml>".getBytes();
        Cell cell = CellUtil.createCell(row, family, column, ts, type, value);
        cells.add(cell);
        Result result = Result.create(cells);
        Result[] results = {result};
        Iterator<Result> iterator = Iterators.forArray(results);

        when(rs.iterator()).thenReturn(iterator);
        when(table.getScanner(any(Scan.class))).thenReturn(rs);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testDefaultTableName() {
        assertThat(dumper.getTableName(), is("documents"));
    }

    @Test
    public void testDefaulFamilyName() {
        assertThat(dumper.getFamilyName(), is("naf"));
    }

    @Test
    public void testDefaultColumnName() {
        assertThat(dumper.getColumnName(), is("annotated"));
    }

    @Test
    public void testRun() throws Exception {
        // redirect stdout to variable
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));
        setUpScan();

        dumper.run();

        String expected = "5b1t-br51-jd3n-54fk.naf : 10\n";
        assertThat(outContent.toString(), is(expected));
        verify(rs).close();
        verify(table).close();

        // undo stdout redirect
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
    }
}