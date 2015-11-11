package fr.finaxys.tutorials.utils.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import fr.finaxys.tutorials.utils.InjectorTests;
import fr.finaxys.tutorials.utils.TimeStampBuilder;

@Category(InjectorTests.class)
public class HBaseAnalysisTest {
	
	private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
			.getLogger(SimpleHBaseInjectorTest.class.getName());
	
	private static final TableName TEST_TABLE = TableName
			.valueOf("testhbaseinjector");
	
	private static final byte[] TEST_FAMILY = Bytes.toBytes("f");
	

	private static HBaseTestingUtility TEST_UTIL = null;
	private static Configuration CONF = null;
	private static Table table = null;
	
	final HBaseDataTypeEncoder hbEncoder = new HBaseDataTypeEncoder();

	private HBaseAnalysis analysis = new HBaseAnalysis();

	@BeforeClass
	public static void setupBeforeClass() throws Exception {
		TEST_UTIL = new HBaseTestingUtility();
		CONF = TEST_UTIL.getConfiguration();

		TEST_UTIL.startMiniCluster();
		table = TEST_UTIL.createTable(TEST_TABLE, new byte[][] { TEST_FAMILY });
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		TEST_UTIL.shutdownMiniCluster();
	}

	@Before
	public void setUp() {
		analysis.setHbaseConfiguration(CONF);
		analysis.setTableName(TEST_TABLE);
		analysis.setColumnFamily(TEST_FAMILY);
	}

	@After
	public void tearDown() {
		analysis = null;
	}

	
	@Test
	public void testTraceCount() {
		//analysis.mkPutExec(analysis, o)
	}
}
