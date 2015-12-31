package fr.finaxys.tutorials.utils.spark.models;

import java.io.Serializable;

public class DataRow implements Serializable {
    
	private static final long serialVersionUID = 5371135155801685329L;
	private String trace;
    private Integer numDay;
    private Integer numTick;
    private Long bestBid;
    private Long bestAsk;
    private String order2;
    private String order1;
    private String dir;
    private String obName;
    private Long validity;
    private Integer quantity;
    private Long id;
    private String type;
    private String extId;
    private String sender;
    private Long nbPricesFixed;
    private Long lastFixedPrice;
    private Long highestPrice;
    private Long lowestPrice;
    private  Long firstFixedPrice;
    private String orderExtId;
    private Long timestamp;
    private String direction;
    private Long price;
    private Integer executed;
    private Long cash;
    private Integer agentRefId;
    private String agentName;
    private Boolean isMarketMaker;
    private String details ;
    private String Invest ;


    public Long getBestBid() {
        return bestBid;
    }

    public void setBestBid(Long bestBid) {
        this.bestBid = bestBid;
    }

    public String getTrace() {
        return trace;
    }

    public void setTrace(String trace) {
        this.trace = trace;
    }

    public Integer getNumDay() {
        return numDay;
    }

    public void setNumDay(Integer numDay) {
        this.numDay = numDay;
    }

    public Integer getNumTick() {
        return numTick;
    }

    public void setNumTick(Integer numTick) {
        this.numTick = numTick;
    }

    public Long getBestAsk() {
        return bestAsk;
    }

    public void setBestAsk(Long bestAsk) {
        this.bestAsk = bestAsk;
    }

    public String getOrder2() {
        return order2;
    }

    public void setOrder2(String order2) {
        this.order2 = order2;
    }

    public String getOrder1() {
        return order1;
    }

    public void setOrder1(String order1) {
        this.order1 = order1;
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public String getObName() {
        return obName;
    }

    public void setObName(String obName) {
        this.obName = obName;
    }

    public Long getValidity() {
        return validity;
    }

    public void setValidity(Long validity) {
        this.validity = validity;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getExtId() {
        return extId;
    }

    public void setExtId(String extId) {
        this.extId = extId;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public Long getNbPricesFixed() {
        return nbPricesFixed;
    }

    public void setNbPricesFixed(Long nbPricesFixed) {
        this.nbPricesFixed = nbPricesFixed;
    }

    public Long getLastFixedPrice() {
        return lastFixedPrice;
    }

    public void setLastFixedPrice(Long lastFixedPrice) {
        this.lastFixedPrice = lastFixedPrice;
    }

    public Long getHighestPrice() {
        return highestPrice;
    }

    public void setHighestPrice(Long highestPrice) {
        this.highestPrice = highestPrice;
    }

    public Long getLowestPrice() {
        return lowestPrice;
    }

    public void setLowestPrice(Long lowestPrice) {
        this.lowestPrice = lowestPrice;
    }

    public Long getFirstFixedPrice() {
        return firstFixedPrice;
    }

    public void setFirstFixedPrice(Long firstFixedPrice) {
        this.firstFixedPrice = firstFixedPrice;
    }

    public String getOrderExtId() {
        return orderExtId;
    }

    public void setOrderExtId(String orderExtId) {
        this.orderExtId = orderExtId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public Long getPrice() {
        return price;
    }

    public void setPrice(Long price) {
        this.price = price;
    }

    public Integer getExecuted() {
        return executed;
    }

    public void setExecuted(Integer executed) {
        this.executed = executed;
    }

    public Long getCash() {
        return cash;
    }

    public void setCash(Long cash) {
        this.cash = cash;
    }

    public Integer getAgentRefId() {
        return agentRefId;
    }

    public void setAgentRefId(Integer agentRefId) {
        this.agentRefId = agentRefId;
    }

    public String getAgentName() {
        return agentName;
    }

    public void setAgentName(String agentName) {
        this.agentName = agentName;
    }

    public Boolean getIsMarketMaker() {
        return isMarketMaker;
    }

    public void setIsMarketMaker(Boolean isMarketMaker) {
        this.isMarketMaker = isMarketMaker;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    public String getInvest() {
        return Invest;
    }

    public void setInvest(String invest) {
        Invest = invest;
    }
}
