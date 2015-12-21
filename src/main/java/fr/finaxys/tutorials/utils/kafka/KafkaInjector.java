package fr.finaxys.tutorials.utils.kafka;

import fr.finaxys.tutorials.utils.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import v13.*;
import v13.agents.Agent;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Created by finaxys on 12/21/15.
 */
public class KafkaInjector implements AtomDataInjector {
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaInjector.class);
    private Producer<String, String> producer;
    private String topic;
    private AtomConfiguration atomConfiguration ;
    private TimeStampBuilder tsb ;
    private int nb_order = 0;

    public KafkaInjector(AtomConfiguration atomConfiguration){
        this.atomConfiguration = atomConfiguration ;
        topic = atomConfiguration.getKafkaTopic();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,atomConfiguration.getKafkaBoot());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put("request.timeout.ms", 100);
        producer = new KafkaProducer(props);
    }

    @Override
    public void closeOutput()  {
        producer.close();
    }

    @Override
    public void createOutput() {
        //TODO
    }

    @Override
    public TimeStampBuilder getTimeStampBuilder() {
        return tsb;
    }

    @Override
    public void setTimeStampBuilder(TimeStampBuilder tsb) {
        this.tsb = tsb ;
    }

    @Override
    public void sendAgent(Agent a, Order o, PriceRecord pr)  {
        producer.send(new ProducerRecord<String, String>(topic, "Agent;" + a.name + ";" + a.cash + ";" + o.obName + ";" + a.getInvest(o.obName) + ";" + (pr != null ? Long.valueOf(pr.price) : "none")));
    }

    @Override
    public void sendPriceRecord(PriceRecord pr, long bestAskPrice, long bestBidPrice)  {
        producer.send(new ProducerRecord<String, String>(topic,"Price;" + pr + ";" + bestAskPrice + ";" + bestBidPrice));
    }

    @Override
    public void sendAgentReferential(List<AgentReferentialLine> referencial)  {

    }

    @Override
    public void sendOrder(Order o)  {
        ++nb_order;
        producer.send(new ProducerRecord<String, String>(topic, (o.toString()/*+displayTimestamp()*/)));
    }

    @Override
    public void sendTick(Day day, Collection<OrderBook> orderbooks)  {
        LOGGER.info(day.currentTick() + " >> " + nb_order);
        nb_order = 0;

        Iterator<OrderBook> i$ = orderbooks.iterator();

        while(i$.hasNext()) {
            OrderBook ob = (OrderBook)i$.next();
            StringBuilder sb = new StringBuilder();
            sb.append("Tick;").append(day.currentPeriod().currentTick()).append(";");
            sb.append(ob.obName).append(";" + (ob.ask.size() > 0?Long.valueOf(((LimitOrder)ob.ask.first()).price):"0"));
            sb.append(";").append(ob.bid.size() > 0?Long.valueOf(((LimitOrder)ob.bid.first()).price):"0");
            sb.append(";").append(ob.lastFixedPrice != null?Long.valueOf(ob.lastFixedPrice.price):"0").append(";");
            producer.send(new ProducerRecord<String, String>(topic, sb.toString()));
        }
    }

    @Override
    public void sendDay(int nbDays, Collection<OrderBook> orderbooks)  {
            Iterator<OrderBook> i$ = orderbooks.iterator();

            while(i$.hasNext()) {
                OrderBook ob = (OrderBook) i$.next();
                StringBuilder sb = new StringBuilder();
                sb.append(ob.obName).append(";").append(ob.firstPriceOfDay);
                sb.append(";").append(ob.lowestPriceOfDay).append(";");
                sb.append(ob.highestPriceOfDay).append(";").append(ob.lastPriceOfDay);
                sb.append(";").append(ob.numberOfPricesFixed).append(";");
                producer.send(new ProducerRecord<String, String>(topic, "Day;" + nbDays + ";" + sb.toString()));
            }
    }

    @Override
    public void sendExec(Order o)  {
        producer.send(new ProducerRecord<String, String>(topic, "Exec;" + o.sender.name + "-" + o.extId));
    }
}
