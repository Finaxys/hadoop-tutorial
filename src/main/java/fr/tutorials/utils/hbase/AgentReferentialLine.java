package fr.tutorials.utils.hbase;

import com.sun.istack.NotNull;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class AgentReferentialLine {

  private final int agentRefId;
  private final String agentName;
  private final boolean isMarketMaker;
  private final String details = "No details available";

  public AgentReferentialLine(int agentRefId, @NotNull String agentName) {
    this.agentRefId = agentRefId;
    this.agentName = agentName;
    this.isMarketMaker = agentName.equals("mm");
  }

  @NotNull
  public Put toPut(@NotNull HBaseDataTypeEncoder encoder, @NotNull byte[] columnF, long ts) {
    Put p = new Put(Bytes.toBytes(agentRefId + "R"), ts);
    p.addColumn(columnF, Bytes.toBytes("agentRefId"), encoder.encodeInt(agentRefId));
    p.addColumn(columnF, Bytes.toBytes("agentName"), encoder.encodeString(agentName));
    p.addColumn(columnF, Bytes.toBytes("isMarketMaker"), encoder.encodeBoolean(isMarketMaker));
    p.addColumn(columnF, Bytes.toBytes("details"), encoder.encodeString(details));
    return p;
  }

  @Override
  public String toString() {
    return "AgentReferentialLine{" +
        "agentRefId=" + agentRefId +
        ", agentName='" + agentName + '\'' +
        ", isMarketMaker=" + isMarketMaker +
        ", details='" + details + '\'' +
        '}';
  }
}
