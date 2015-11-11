package fr.finaxys.tutorials.utils;

import com.sun.istack.NotNull;

import fr.finaxys.tutorials.utils.hbase.HBaseDataTypeEncoder;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class AgentReferentialLine {

  public final int agentRefId;
  public final String agentName;
  public final boolean isMarketMaker;
  public final String details = "No details available";

  public AgentReferentialLine(int agentRefId, @NotNull String agentName) {
    this.agentRefId = agentRefId;
    this.agentName = agentName;
    this.isMarketMaker = agentName.equals("mm");
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
