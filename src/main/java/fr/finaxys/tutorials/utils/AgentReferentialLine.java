package fr.finaxys.tutorials.utils;

import com.sun.istack.NotNull;

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
