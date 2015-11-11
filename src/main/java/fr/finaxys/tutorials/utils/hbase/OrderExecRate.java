package fr.finaxys.tutorials.utils.hbase;


public class OrderExecRate {
	
//	private byte[] columnFamily;
//	
//	public OrderExecRate(byte[] columnFamily) {
//		this.columnFamily = columnFamily;
//	}
//	
//	public static class Map
//    extends TableMapper<Text, LongWritable> {
//
//    public static enum Counters {ROWS, SHAKESPEAREAN};
//    private Random rand;
//
//    /**
//     * Determines if the message pertains to Shakespeare.
//     */
//    private boolean containsShakespear(String msg) {
//      return rand.nextBoolean();
//    }
//
//    @Override
//    protected void setup(Context context) {
//      rand = new Random(System.currentTimeMillis());
//    }
//
//    @Override
//      protected void map(
//          ImmutableBytesWritable rowkey,
//          Result result,
//          Context context) {
//      byte[] b = result.getColumnLatest(
//        TwitsDAO.TWITS_FAM,
//        TwitsDAO.TWIT_COL).getValue();
//      if (b == null) return;
//
//      String msg = Bytes.toString(b);
//      if (msg.isEmpty()) return;
//
//      context.getCounter(Counters.ROWS).increment(1);
//      if (containsShakespear(msg))
//        context.getCounter(Counters.SHAKESPEAREAN).increment(1);
//    }
//  }
//
//  public void run(String[] args) throws Exception {
//    Configuration conf = HBaseConfiguration.create();
//    Job job = Job.getInstance(conf, "Order Exec Rate");
//    job.setJarByClass(OrderExecRate.class);
//
//    Scan scan = new Scan();
//    scan.addColumn(columnFamily, AtomHBaseHelper.Q_AGENT_NAME);
//    TableMapReduceUtil.initTableMapperJob(
//      Bytes.toString(TwitsDAO.TABLE_NAME),
//      scan,
//      Map.class,
//      ImmutableBytesWritable.class,
//      Result.class,
//      job);
//
//    job.setOutputFormatClass(NullOutputFormat.class);
//    job.setNumReduceTasks(0);
//    System.exit(job.waitForCompletion(true) ? 0 : 1);
//  }
}
