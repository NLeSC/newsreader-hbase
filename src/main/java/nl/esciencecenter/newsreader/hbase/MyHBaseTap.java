package nl.esciencecenter.newsreader.hbase;
/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.hbase.HBaseAbstractScheme;
import cascading.hbase.HBaseScheme;
import cascading.hbase.HBaseTapCollector;
import cascading.hbase.helper.TableInputFormat;
import cascading.property.AppProps;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 * The MyHBaseTap class is a {@link Tap} subclass. It is used in conjunction with
 * the {@link HBaseScheme} to allow for the reading and writing
 * of data to and from a HBase cluster.
 */
public class MyHBaseTap extends Tap<Configuration, RecordReader, OutputCollector>
{

    static
    {
        // add cascading-hbase release to frameworks
        Properties properties = new Properties();
        InputStream stream = MyHBaseTap.class.getClassLoader().getResourceAsStream( "cascading/framework.properties" );
        if( stream != null )
        {
            try
            {
                properties.load( stream );
                stream.close();
            }
            catch( IOException exception )
            {
                // ingore
            }
        }
        String framework = properties.getProperty( "name" );
        AppProps.addApplicationFramework(null, framework);
    }

    /** Field SCHEME */
    public static final String SCHEME = "hbase";
    /** Field LOG */
    private static final Logger LOG = LoggerFactory.getLogger(MyHBaseTap.class);
    private final String id = UUID.randomUUID().toString();
    /** Field hBaseAdmin */
    private transient HBaseAdmin hBaseAdmin;
    private String tableName;

    private int uniqueId;

    /**
     * Constructor MyHBaseTap creates a new MyHBaseTap instance.
     *
     * @param tableName       of type String
     * @param HBaseFullScheme of type HBaseFullScheme
     */
    public MyHBaseTap( String tableName, HBaseAbstractScheme HBaseFullScheme )
    {
        this( tableName, HBaseFullScheme, SinkMode.KEEP );
    }

    /**
     * Instantiates a new hbase tap.
     *
     * @param tableName       the table name
     * @param HBaseFullScheme the h base full scheme
     * @param uniqueId        the uniqueId (0 if no id given)
     */
    public MyHBaseTap( String tableName, HBaseAbstractScheme HBaseFullScheme, int uniqueId )
    {
        this( tableName, HBaseFullScheme, SinkMode.KEEP, uniqueId );
    }

    /**
     * Instantiates a new h base tap.
     *
     * @param tableName       the table name
     * @param HBaseFullScheme the h base full scheme
     * @param sinkMode        the sink mode
     */
    public MyHBaseTap( String tableName, HBaseAbstractScheme HBaseFullScheme, SinkMode sinkMode )
    {
        this( tableName, HBaseFullScheme, sinkMode, 0 );
    }

    /**
     * Constructor MyHBaseTap creates a new MyHBaseTap instance.
     *
     * @param tableName       of type String
     * @param HBaseFullScheme of type HBaseFullScheme
     * @param sinkMode        of type SinkMode
     * @param uniqueId        the uniqueId (0 if no id given)
     */
    public MyHBaseTap( String tableName, HBaseAbstractScheme HBaseFullScheme, SinkMode sinkMode, int uniqueId )
    {
        super( HBaseFullScheme, sinkMode );
        this.tableName = tableName;
        this.uniqueId = uniqueId;
    }

    public Path getPath()
    {
        return new Path( SCHEME + "://" + tableName.replaceAll( ":", "_" ) );
    }

    @Override
    public TupleEntryIterator openForRead( FlowProcess<? extends Configuration> flowProcess, RecordReader input ) throws IOException
    {
        return new HadoopTupleEntrySchemeIterator( flowProcess, this, input );
    }

    @Override
    public TupleEntryCollector openForWrite( FlowProcess<? extends Configuration> flowProcess, OutputCollector output ) throws IOException
    {
        HBaseTapCollector hBaseCollector = new HBaseTapCollector( flowProcess, this );
        hBaseCollector.prepare();
        return hBaseCollector;
    }

    private HBaseAdmin getHBaseAdmin( Configuration conf ) throws IOException
    {
        Thread.currentThread().setContextClassLoader( HBaseConfiguration.class.getClassLoader() );
        if( hBaseAdmin == null )
            hBaseAdmin = new HBaseAdmin( HBaseConfiguration.create( conf ) );
        return hBaseAdmin;
    }

    private void obtainToken( Configuration conf )
    {
//        conf.addResource("/etc/hadoop/conf/core-site.xml");
//        conf.addResource("/etc/hadoop/conf/hdfs-site.xml");
//        conf.addResource("/etc/hbase/conf/hbase-site.xml");
//
//        conf.set("hadoop.security.authentication", "kerberos");
//        conf.set("hadoop.security.authorization", "true");
        
    	JobConf jobConf = new JobConf( conf );
    	try {
			TableMapReduceUtil.initCredentials(jobConf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
//        try {
//        UserGroupInformation.setConfiguration(conf);
//        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
//        if( User.isHBaseSecurityEnabled(conf) )
//        {
//            JobConf jobConf = new JobConf( conf );
//            TableMapReduceUtil.initCredentials(jobConf);
//            String user = loginUser.getUserName();//jobConf.getUser();
//            LOG.info( "obtaining HBase token for: {}", user );
//            //try
//            //{
//                UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
//                user = currentUser.getUserName();
//                LOG.info("user op de andere manier: " + user);
//                //Credentials credentials = jobConf.getCredentials();
//                Credentials credentials = currentUser.getCredentials();
//                for( Token token : currentUser.getTokens() )
//                {
//                    LOG.info("Token {} is available", token);
//                    //there must be HBASE_AUTH_TOKEN exists, if not bad thing will happen, it's must be generated during job submission.
//                    if( "HBASE_AUTH_TOKEN".equalsIgnoreCase( token.getKind().toString() ) )
//                        credentials.addToken( token.getKind(), token );
//                }
//                jobConf.setCredentials(credentials);
//                jobConf.setUser(user);
//
//            }
//
//        }
//        catch( IOException e )
//        {
//            //throw new TapException( "Unable to obtain HBase auth token for " + loginUser, e );
//            e.printStackTrace();
//        }
    }

    @Override
    public boolean resourceExists( Configuration conf ) throws IOException
    {
        return getHBaseAdmin( conf ).tableExists( tableName );
    }

    @Override
    public long getModifiedTime( Configuration conf ) throws IOException
    {
        return System.currentTimeMillis(); // currently unable to find last mod time on a table
    }

    @Override
    public void sinkConfInit( FlowProcess<? extends Configuration> flowProcess, Configuration conf )
    {
        LOG.debug( "sinking to table: {}", tableName );
        obtainToken( conf );
        try
        {
            createResource( conf );
        }
        catch( IOException e )
        {
            throw new RuntimeException( "failed to create table '" + tableName + "'", e );
        }

        conf.set( TableOutputFormat.OUTPUT_TABLE, tableName );
        super.sinkConfInit( flowProcess, conf );
    }

    @Override
    public void sourceConfInit( FlowProcess<? extends Configuration> flowProcess, Configuration conf )
    {
        LOG.debug( "sourcing from table: {}", tableName );
        conf.set( "mapred.input.format.class", TableInputFormat.class.getName() );
        conf.set( TableInputFormat.INPUT_TABLE, tableName );
        obtainToken( conf );
        super.sourceConfInit( flowProcess, conf );
    }

    @Override
    public boolean equals( Object object )
    {
        if( object == null )
            return false;
        if( this == object )
            return true;
        if( !( object instanceof MyHBaseTap ) )
            return false;
        if( !super.equals( object ) )
            return false;

        MyHBaseTap tap = (MyHBaseTap) object;

        if( tableName == null ? tap.tableName != null : !tableName.equals( tap.tableName ) )
            return false;

        return uniqueId == tap.uniqueId;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + ( tableName == null ? 0 : tableName.hashCode() );
        return result;
    }

    @Override
    public String toString()
    {
        return getPath().toString();
    }

    @Override
    public boolean createResource( Configuration conf ) throws IOException
    {
        HBaseAdmin hBaseAdmin = getHBaseAdmin( conf );
        if( hBaseAdmin.tableExists( tableName ) )
            return true;

        LOG.info( "creating hbase table: {}", tableName );

        HTableDescriptor tableDescriptor = new HTableDescriptor( TableName.valueOf(tableName) );

        String[] familyNames = ( (HBaseAbstractScheme) getScheme() ).getFamilyNames();

        for( String familyName : familyNames )
            tableDescriptor.addFamily( new HColumnDescriptor( familyName ) );

        hBaseAdmin.createTable( tableDescriptor );
        return true;
    }

    @Override
    public boolean deleteResource( Configuration conf ) throws IOException
    {
        try
        {
            // eventually keep table meta-data to source table create
            HBaseAdmin hBaseAdmin = getHBaseAdmin( conf );

            if( !hBaseAdmin.tableExists( tableName ) )
                return true;

            LOG.debug( "deleting hbase table: {}", tableName );
            hBaseAdmin.disableTable( tableName );
            hBaseAdmin.deleteTable( tableName );
            return true;

        }
        catch( Exception e )
        {
            LOG.error( "error while deleting table {} {}", tableName, e );
            return false;
        }
    }

    @Override
    public String getIdentifier()
    {
        return id;
    }

}
