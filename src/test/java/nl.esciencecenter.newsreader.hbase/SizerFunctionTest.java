package nl.esciencecenter.newsreader.hbase;

import java.util.ArrayList;
import java.util.Iterator;

import org.junit.Test;

import cascading.CascadingTestCase;
import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleListCollector;

public class SizerFunctionTest extends CascadingTestCase{

	@Test
	public void testOperate_withContent_contentSizeNonZero() {
		Function sizer = new SizerFunction(new Fields("id", "size"));
		
		Tuple[] arguments = new Tuple[]{new Tuple( "doc_1", "FoO" ), new Tuple( "doc_2", "BARF" ), new Tuple( "doc_3", "crunch" )
	    };
		
		 ArrayList<Tuple> expectResults = new ArrayList<Tuple>();
		    expectResults.add( new Tuple( "doc_1", 3 ) );
		    expectResults.add( new Tuple( "doc_2", 4 ) );
		    expectResults.add( new Tuple( "doc_3", 6 ) );

		    TupleListCollector collector = invokeFunction( sizer, arguments, Fields.ALL );
		    Iterator<Tuple> it = collector.iterator();
		    ArrayList<Tuple> results = new ArrayList<Tuple>();

		    while( it.hasNext() ) results.add( it.next() );

		    assertEquals( "Scrub result is not expected", expectResults, results );
	}

}
