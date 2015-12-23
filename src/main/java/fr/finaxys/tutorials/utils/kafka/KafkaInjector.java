package fr.finaxys.tutorials.utils.kafka;

import fr.finaxys.tutorials.utils.AgentReferentialLine;
import fr.finaxys.tutorials.utils.AtomConfiguration;
import fr.finaxys.tutorials.utils.AtomDataInjector;
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
    private AtomConfiguration atomConfiguration;

    public KafkaInjector(AtomConfiguration atomConfiguration) {
        this.atomConfiguration = atomConfiguration;
        topic = atomConfiguration.getKafkaTopic();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, atomConfiguration.getKafkaBoot());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put("request.timeout.ms", 100);
        producer = new KafkaProducer(props);
    }

    @Override
    public void closeOutput() {
        producer.close();
    }

    @Override
    public void createOutput() {
        //TODO
    }


    @Override
    public void sendAgent(long ts, Agent a, Order o, PriceRecord pr) {
        StringBuilder sb = new StringBuilder();
        sb.append("Agent").append(";");
        sb.append(a.name).append(";");
        sb.append(a.cash).append(";");
        sb.append(o.obName).append(";");
        sb.append(a.getInvest(o.obName)).append(";");
        sb.append((pr != null ? Long.valueOf(pr.price) : "none")).append(";");
        sb.append(ts);

        producer.send(new ProducerRecord<String, String>(topic, sb.toString()));
    }

    @Override
    public void sendPriceRecord(long ts, PriceRecord pr, long bestAskPrice, long bestBidPrice) {
        StringBuilder sb = new StringBuilder();
        sb.append("Price").append(";");
        sb.append(pr).append(";");
        sb.append(bestAskPrice).append(";");
        sb.append(bestBidPrice).append(";");
        sb.append(ts);

        producer.send(new ProducerRecord<String, String>(topic, sb.toString()));
    }

    @Override
    public void sendAgentReferential(long ts, List<AgentReferentialLine> referencial) {

    }

    @Override
    public void sendOrder(long ts, Order o) {
        StringBuilder sb = new StringBuilder();
        sb.append(o.toString()).append(";");
        sb.append(ts);

        producer.send(new ProducerRecord<String, String>(topic, sb.toString()));
    }

    @Override
    public void sendTick(long ts, Day day, Collection<OrderBook> orderbooks) {

        Iterator<OrderBook> i$ = orderbooks.iterator();

        while (i$.hasNext()) {
            OrderBook ob = i$.next();
            StringBuilder sb = new StringBuilder();
            sb.append("Tick").append(";");
            sb.append(day.currentPeriod().currentTick()).append(";");
            sb.append(ob.obName).append(";");
            sb.append(ob.ask.size() > 0 ? Long.valueOf(((LimitOrder) ob.ask.first()).price) : "0").append(";");
            sb.append(ob.bid.size() > 0 ? Long.valueOf(((LimitOrder) ob.bid.first()).price) : "0").append(";");
            sb.append(ob.lastFixedPrice != null ? Long.valueOf(ob.lastFixedPrice.price) : "0").append(";");
            sb.append(ts);

            producer.send(new ProducerRecord<String, String>(topic, sb.toString()));
        }
    }

    @Override
    public void sendDay(long ts, int nbDays, Collection<OrderBook> orderbooks) {
        Iterator<OrderBook> i$ = orderbooks.iterator();

        while (i$.hasNext()) {
            OrderBook ob = i$.next();
            StringBuilder sb = new StringBuilder();
            sb.append("Day").append(";");
            sb.append(nbDays).append(";");
            sb.append(ob.obName).append(";");
            sb.append(ob.firstPriceOfDay).append(";");
            sb.append(ob.lowestPriceOfDay).append(";");
            sb.append(ob.highestPriceOfDay).append(";");
            sb.append(ob.lastPriceOfDay).append(";");
            sb.append(ob.numberOfPricesFixed).append(";");
            sb.append(ts);
            producer.send(new ProducerRecord<String, String>(topic, sb.toString()));
        }
    }

    @Override
    public void sendExec(long ts, Order o) {
        StringBuilder sb = new StringBuilder();
        sb.append("Exec").append(";");
        sb.append(o.sender.name).append("-").append(o.extId).append(";");
        sb.append(ts);

        producer.send(new ProducerRecord<String, String>(topic, sb.toString()));
    }
}
