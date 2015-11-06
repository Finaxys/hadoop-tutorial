package fr.finaxys.tutorials.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;

//import org.testng.annotations.AfterMethod;
//import org.testng.annotations.BeforeMethod;
//import org.testng.annotations.DataProvider;
//import org.testng.annotations.Test;
//
//import org.testng.Assert;

// @Test
@Category(InjectorTests.class)
@RunWith(DataProviderRunner.class)
public class TimeStampBuilderTest {
	private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
			.getLogger(AtomLogger.class.getName());

	private static TimeStampBuilder tsb = new TimeStampBuilder();

	// @BeforeMethod
	@Before
	public void setUp() throws Exception {
	}

	// @AfterMethod
	@After
	public void tearDown() {
	}

	public static long callNextTimeStampXTime(long nb) {
		while (nb > 0) {
			tsb.nextTimeStamp();
			nb--;
		}
		return tsb.getTimeStamp();
	}

	@DataProvider
	public static Object[][] myDataProvider() {
		return new Object[][] {
				{ tsb.baseTimeStampForCurrentTick(), tsb.getTimeStamp(), true },
				{ tsb.baseTimeStampForCurrentTick() + tsb.getTimePerOrder(),
						tsb.nextTimeStamp(), true },
				{ callNextTimeStampXTime(tsb.getNbMaxOrderPerTick()),
						tsb.baseTimeStampForNextTick(), false
				// le callnectimestampxtime ne doit pas etre supérieur au
				// basetimestampnextick
				} };
	}

	@Test
	@UseDataProvider("myDataProvider")
	public void timestampFor20Ticks(long expected, long received, boolean match)
			throws Exception {
		// loadConfig
		String dateBegin = "09/13/1986";
		String openHourStr = "9:00";
		String closeHourStr = "17:30";
		tsb.resetFromString(dateBegin, openHourStr, closeHourStr);
		tsb.setNbTickMax(20);
		tsb.setNbAgents(2);
		tsb.setNbOrderBooks(2);
		tsb.init();
		tsb.setTimeStamp(tsb.baseTimeStampForCurrentTick());
		LOGGER.info("timestamp " + tsb.getTimeStamp());
		LOGGER.info("bla " + tsb.baseTimeStampForCurrentTick());
		// Assert.assertEquals(match, expected == received);

	}

	// @Test(dataProvider = "myDataProvider")
	@Test
	@UseDataProvider("myDataProvider")
	public void timestampFor30kTicks(long expected, long received, boolean match)
			throws Exception {
		// loadConfig
		String dateBegin = "09/13/1986";
		String openHourStr = "9:00";
		String closeHourStr = "17:30";
		tsb.resetFromString(dateBegin, openHourStr, closeHourStr);
		tsb.setNbTickMax(30000);
		tsb.setNbAgents(2);
		tsb.setNbOrderBooks(2);
		tsb.init();
		tsb.setTimeStamp(tsb.baseTimeStampForCurrentTick());
		// Assert.assertEquals(match, expected == received);
	}

	@Test
	public void timePerOrder() throws Exception {
		String dateBegin = "09/13/1986";
		String openHourStr = "9:00";
		String closeHourStr = "17:30";
		tsb.resetFromString(dateBegin, openHourStr, closeHourStr);
		tsb.setNbTickMax(20);
		tsb.setNbAgents(2);
		tsb.setNbOrderBooks(2);
		tsb.init();
		tsb.setTimeStamp(tsb.baseTimeStampForCurrentTick());
//		Assert.assertEquals(
//				tsb.baseTimeStampForNextTick()
//						- (tsb.baseTimeStampForCurrentTick()),
//				tsb.getNbMaxOrderPerTick() * (tsb.getTimePerOrder()));
	}

	// @Test
	public void shouldNotByPassBaseTimeStampForNextTick() throws Exception {
		String dateBegin = "09/13/1986";
		String openHourStr = "9:00";
		String closeHourStr = "17:30";
		tsb.resetFromString(dateBegin, openHourStr, closeHourStr);
		tsb.setNbTickMax(20);
		tsb.setNbAgents(2);
		tsb.setNbOrderBooks(2);
		tsb.init();
		tsb.setTimeStamp(tsb.baseTimeStampForCurrentTick());
		LOGGER.info("Next tick = " + tsb.baseTimeStampForNextTick());
		LOGGER.info("addition = "
				+ (tsb.getTimeStamp() + tsb.getNbMaxOrderPerTick()
						* (tsb.getTimePerOrder())));
		LOGGER.info("timeStamp = " + tsb.getTimeStamp());
		LOGGER.info("add = " + tsb.getNbMaxOrderPerTick()
				* (tsb.getTimePerOrder()));
//		Assert.assertTrue(tsb.baseTimeStampForNextTick() <= (tsb.getTimeStamp() + tsb
//				.getNbMaxOrderPerTick() * (tsb.getTimePerOrder())));

	}

	@Test
	public void shouldReachNextTick() throws Exception {
		TimeStampBuilder t = new TimeStampBuilder("01/01/2000", "10:00",
				"11:00", 6, 2, 2);
		t.init();
		t.setTimeStamp(t.baseTimeStampForCurrentTick());
		long hour = t.baseTimeStampForNextTick()
				- t.baseTimeStampForCurrentTick();
		LOGGER.info("hour = " + hour);
//		Assert.assertEquals(600000,
//				t.baseTimeStampForNextTick() - t.baseTimeStampForCurrentTick());
		hour = t.getNbMaxOrderPerTick() * t.getTimePerOrder();
		LOGGER.info("hourbis = " + hour);
//		Assert.assertEquals(600000,
//				t.getNbMaxOrderPerTick() * t.getTimePerOrder());
		hour = t.getTimeStamp() + t.getTimePerOrder()
				* t.getNbMaxOrderPerTick();
		LOGGER.info("hourbisbis = " + hour);
		LOGGER.info("nextTick = " + t.baseTimeStampForNextTick());
//		Assert.assertEquals(
//				t.getTimeStamp() + t.getTimePerOrder()
//						* t.getNbMaxOrderPerTick(),
//				t.baseTimeStampForNextTick());
		LOGGER.info("t " + t.getNbMaxOrderPerTick());
	}
}