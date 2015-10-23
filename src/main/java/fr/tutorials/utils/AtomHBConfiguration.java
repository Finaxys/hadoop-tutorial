package fr.tutorials.utils;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public class AtomHBConfiguration {

  private static final Logger LOGGER = Logger.getLogger(AtomHBConfiguration.class.getName());

  //Business Data
  private List<String> agents = new ArrayList<>();
  private List<String> orderBooks = new ArrayList<>();

  private int dayGap;
  private int tickOpening;
  private int tickContinuous;
  private int tickClosing;
  private int days;


  //HBase - MultiThreading - buff options
  private int worker;
  private int flushRatio;
  private int bufferSize;
  private int stackPuts;
  private boolean autoFlush;
  private byte[] cfName;
  private String tableName = "trace";

  //App data
  private long startTime;
  private boolean outHbase;
  private boolean outSystem;
  private String outFilePath;
  private boolean replay;
  private String replaySource;
  private boolean outFile;


  public AtomHBConfiguration() throws IOException {
    load();
  }

  private void load() throws IOException {
    FileInputStream propFile = new FileInputStream("properties.txt");
    Properties p = new Properties(System.getProperties());
    p.load(propFile);
    System.setProperties(p);

    // Get agents & orderbooks
    String obsym = System.getProperty("atom.orderbooks", "");
    LOGGER.info("obsym = " + obsym);
    assert obsym != null;
    String agsym = System.getProperty("atom.agents", "");
    assert agsym != null;

    this.agents = Arrays.asList(System.getProperty("symbols.agents." + agsym, "").split("\\s*,\\s*"));
    this.orderBooks = Arrays.asList(System.getProperty("symbols.orderbooks." + obsym, "").split("\\s*,\\s*"));

    if (agents.isEmpty() || orderBooks.isEmpty()) {
      LOGGER.log(Level.SEVERE, "Agents/Orderbooks not set");
      throw new IOException("agents/orderbooks not set");
    }

    this.tableName = System.getProperty("hbase.table", "trace");
    this.cfName = Bytes.toBytes(System.getProperty("hbase.cf", "cf"));
    assert cfName != null;
    this.outHbase = System.getProperty("simul.output.hbase", "true").equals("true");
    this.outFile = Boolean.parseBoolean(System.getProperty("simul.output.file", "false"));
    this.outFilePath = System.getProperty("simul.output.file.path", "outPutAtom.log");
    this.outSystem = System.getProperty("simul.output.standard", "false").equals("false");
    this.dayGap = Integer.parseInt(System.getProperty("simul.day.startDay", "1")) - 1;

    this.startTime = System.currentTimeMillis();
    this.worker = Integer.parseInt(System.getProperty("simul.worker", "10"));
    this.flushRatio = Integer.parseInt(System.getProperty("simul.flushRatio", "1000"));
    this.bufferSize = Integer.parseInt(System.getProperty("simul.bufferSize", "10000"));
    this.autoFlush = Boolean.parseBoolean(System.getProperty("hbase.autoFlush", "false"));
    this.stackPuts = Integer.parseInt(System.getProperty("hbase.stackputs", "1000"));

    this.tickOpening = Integer.parseInt(System.getProperty("simul.tick.opening", "0"));
    this.tickContinuous = Integer.parseInt(System.getProperty("simul.tick.continuous", "10"));
    this.tickClosing = Integer.parseInt(System.getProperty("simul.tick.closing", "0"));
    this.days = Integer.parseInt(System.getProperty("simul.days", "1"));

    this.replay = Boolean.parseBoolean(System.getProperty("simul.replay", "false"));
    this.replaySource = System.getProperty("simul.replay.source", "");

  }

  public String getTableName() {
    return tableName;
  }

  public byte[] getColumnFamily() {
    return cfName;
  }

  public boolean isAutoFlush() {
    return autoFlush;
  }

  public List<String> getAgents() {
    return agents;
  }

  public List<String> getOrderBooks() {
    return orderBooks;
  }

  public int getDayGap() {
    return dayGap;
  }

  public int getWorker() {
    return worker;
  }

  public int getFlushRatio() {
    return flushRatio;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public int getStackPuts() {
    return stackPuts;
  }

  public byte[] getCfName() {
    return cfName;
  }

  public long getStartTime() {
    return startTime;
  }

  public boolean isOutHbase() {
    return outHbase;
  }

  public boolean isOutSystem() {
    return outSystem;
  }

  public String getOutFilePath() {
    return outFilePath;
  }

  public int getTickOpening() {
    return tickOpening;
  }

  public int getDays() {
    return days;
  }

  public int getTickClosing() {
    return tickClosing;
  }

  public int getTickContinuous() {
    return tickContinuous;
  }

  public boolean isReplay() {
    return replay;
  }

  public String getReplaySource() {
    return replaySource;
  }

  public boolean isOutFile() {
    return outFile;
  }
}
