package fr.finaxys.tutorials.utils.parquet;

import fr.finaxys.tutorials.utils.InjectorTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

/**
 * Created by finaxys on 12/2/15.
 */
@Category(InjectorTests.class)
public class AvroParquetConverterTest {
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
            .getLogger(AvroParquetConverterTest.class.getName());
    private static final String FILE_PATH = "parquet-result";
    private static HBaseTestingUtility TEST_UTIL = null;
    private static Configuration CONF = null;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        TEST_UTIL = new HBaseTestingUtility();
        CONF = TEST_UTIL.getConfiguration();
        TEST_UTIL.startMiniCluster();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
        //TEST_UTIL.shutdownMiniMapReduceCluster();
    }

}
