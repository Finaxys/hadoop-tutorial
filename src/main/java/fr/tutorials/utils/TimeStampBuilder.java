package fr.tutorials.utils;

import java.io.FileInputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class TimeStampBuilder
{
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(TimeStampBuilder.class.getName());
    private int nbTickMax;
    private int currentTick = 1;
    private int currentDay = 0;
    private long dateToSeconds = 0L;
    private long openHoursToSeconds;
    private long closeHoursToSeconds;
    private long ratio;
    private long timeStamp;
    private long nbMaxOrderPerTick;
    private long timePerOrder;
    private static final long nbMilliSecDay = 86400000;
    private static final long nbMilliSecHour = 3600000;

    public int getCurrentTick()
    {
        return currentTick;
    }

    public void setCurrentTick(int currentTick)
    {
        this.currentTick = currentTick;
    }

    public int getCurrentDay()
    {
        return currentDay;
    }

    public void setCurrentDay(int currentDay)
    {
        this.currentDay = currentDay;
    }

    public long getTimeStamp()
    {
        return this.timeStamp;
    }

    public void setTimeStamp(long timeStamp)
    {
        this.timeStamp = timeStamp;
    }

    public long getTimePerOrder()
    {
        return timePerOrder;
    }

    public long getDateToSeconds()
    {
        return dateToSeconds;
    }

    public long getOpenHoursToSeconds()
    {
        return openHoursToSeconds;
    }

    public long getCloseHoursToSeconds()
    {
        return closeHoursToSeconds;
    }


    public long getNbMaxOrderPerTick()
    {
        return nbMaxOrderPerTick;
    }

    public TimeStampBuilder()
    {
    }

    public TimeStampBuilder(String dateBegin, String openHourStr, String closeHourStr, String nbTickMaxStr) throws Exception
    {
        convertFromString(dateBegin, openHourStr, closeHourStr, nbTickMaxStr);
    }

    public long baseTimeStampForCurrentTick()
    {
        long baseTimeStampCurrentTick;
        if (currentTick == nbTickMax)
        {
            baseTimeStampCurrentTick = nbMilliSecHour + dateToSeconds + (currentDay - 1) * nbMilliSecDay + openHoursToSeconds + (currentTick - 1) * ratio;
        }
        else
        {
            baseTimeStampCurrentTick = nbMilliSecHour + dateToSeconds + currentDay * nbMilliSecDay + openHoursToSeconds + (currentTick - 1) * ratio;
        }
        return (baseTimeStampCurrentTick);
    }

    public long baseTimeStampForNextTick()
    {
        long baseTimeStampNextTick;
        if (currentTick == nbTickMax)
        {
            baseTimeStampNextTick = nbMilliSecHour + dateToSeconds + (currentDay - 1) * nbMilliSecDay + openHoursToSeconds + (currentTick) * ratio;
        }
        else
        {
            baseTimeStampNextTick = nbMilliSecHour + dateToSeconds + currentDay * nbMilliSecDay + openHoursToSeconds + (currentTick) * ratio;
        }
        return (baseTimeStampNextTick);
    }

    public long nextTimeStamp()
    {
        timeStamp += timePerOrder;
        return (timeStamp);
    }

    /**
     * For test
     *
     * @throws Exception
     */
    protected void loadConfig() throws Exception
    {
        //take the date
        String dateBegin = System.getProperty("simul.time.startdate");
        assert dateBegin != null;

        //take the hours
        String openHourStr = System.getProperty("simul.time.openhour");
        String closeHourStr = System.getProperty("simul.time.closehour");

        //Take the period
        String nbTickMaxStr = System.getProperty("simul.tick.continuous");
        assert nbTickMaxStr != null;

        convertFromString(dateBegin, openHourStr, closeHourStr, nbTickMaxStr);
    }

    protected void convertFromString(String dateBegin, String openHourStr, String closeHourStr, String nbTickMaxStr) throws Exception
    {
        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
        Date date = null;

        date = formatter.parse(dateBegin);
        //LOGGER.info("date = " + date);
        dateToSeconds = date.getTime();
        //LOGGER.info("timestamp à partir du fichier de conf : " + dateToSeconds);

        DateFormat dateFormatter = new SimpleDateFormat("h:mm");
        Date openHour = null;
        Date closeHour = null;
        openHour = (Date) dateFormatter.parse(openHourStr);
        assert openHour != null;
        closeHour = (Date) dateFormatter.parse(closeHourStr);
        assert closeHour != null;
        openHoursToSeconds = openHour.getTime();
        closeHoursToSeconds = closeHour.getTime();

        //LOGGER.info("simul.tick.continuous = " + nbTickMaxStr);
        nbTickMax = Integer.parseInt(nbTickMaxStr);

    }

    protected void init() throws Exception
    {
        ratio = (closeHoursToSeconds - openHoursToSeconds) / (nbTickMax); // +1 to not reach the closehour on the last tick or not +1 but begin at openhour

        LOGGER.info("ratio = " + ratio);

        //calc nb max order between 2 ticks
        nbMaxOrderPerTick = getNbAgents() * getNbOrderBooks() * 2; // * 2
        LOGGER.info("nbmaxorderpertick = " + nbMaxOrderPerTick);
        timePerOrder = (ratio / nbMaxOrderPerTick);
        LOGGER.info("timePerOrder is = " + timePerOrder);
        setTimeStamp(baseTimeStampForCurrentTick());
    }

    private int getNbAgents() throws Exception
    {
        FileInputStream propFile = new FileInputStream("properties.txt");
        Properties p = new Properties(System.getProperties());
        p.load(propFile);
        System.setProperties(p);
        String nbAgentsStr = System.getProperty("symbols.agents.basic");
        assert nbAgentsStr != null;
        //LOGGER.info("nbAgentsStr = " + nbAgentsStr);
        List<String> Str = Arrays.asList(nbAgentsStr.split(","));
        //LOGGER.info("str size = " + Str.size());
        int nbAgents = Str.size();
        return (nbAgents);
    }

    private int getNbOrderBooks()
    {
        String strNbOrderBook= System.getProperty("atom.orderbooks.random");
        return (Integer.parseInt(strNbOrderBook));

        // Old
//        String obsym = System.getProperty("atom.orderbooks", "");
//        assert obsym != null;
//
//        String nbOrderBookStr = System.getProperty("symbols.orderbooks." + obsym);
//        //LOGGER.info("nbOderBookStr = " + nbOrderBookStr);
//        assert nbOrderBookStr != null;
//        List<String> Str = Arrays.asList(nbOrderBookStr.split(","));
//        //LOGGER.info("str order book size = " + Str.size());
//        int nbOrderBook = Str.size();
//        return (nbOrderBook);
    }

}