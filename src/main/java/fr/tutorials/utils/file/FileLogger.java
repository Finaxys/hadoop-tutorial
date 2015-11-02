package fr.tutorials.utils.file;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Iterator;

import fr.tutorials.utils.TimeStampBuilder;
import v13.Day;
import v13.LimitOrder;
import v13.Order;
import v13.OrderBook;
import v13.PriceRecord;
import v13.agents.Agent;

public class FileLogger extends v13.Logger{
    private PrintStream pw = null;
    private TimeStampBuilder tsb;
    private int nb_order = 0;

    public FileLogger(String filename) {
        try {
            this.pw = new PrintStream(filename);
            tsb = new TimeStampBuilder();
            tsb.loadConfig();
            tsb.init();
        } catch (Exception var3) {
            var3.printStackTrace();
            System.exit(1);
        }
    }
    
    public FileLogger(PrintStream pw) {
        try {
            this.pw = pw;
            tsb = new TimeStampBuilder();
            tsb.loadConfig();
            tsb.init();
        } catch (Exception var3) {
            var3.printStackTrace();
            System.exit(1);
        }
    }

    public void println(String s) {
        if(this.pw != null) {
            this.pw.println(s);
            this.pw.flush();
        }

    }

    public void println() {
        this.print("\n");
    }

    public void print(String s) {
        if(this.pw != null) {
            this.pw.print(s);
            this.pw.flush();
        }

    }

    public void error(Exception e) {
        if(this.pw != null) {
            this.println("#ERROR;" + e.getMessage());
        }

    }

    public void order(Order o) {
        if(this.pw != null) {
            ++nb_order;
            this.print(o.toString()/*+displayTimestamp()*/);
        }

    }

    public void day(int nbDays, Collection<OrderBook> orderbooks) {
        if(this.pw != null) {
            tsb.setCurrentDay(nbDays);

            Iterator<OrderBook> i$ = orderbooks.iterator();

            while(i$.hasNext()) {
                OrderBook ob = (OrderBook)i$.next();
                StringBuilder sb = new StringBuilder();
                sb.append(ob.obName).append(";").append(ob.firstPriceOfDay);
                sb.append(";").append(ob.lowestPriceOfDay).append(";");
                sb.append(ob.highestPriceOfDay).append(";").append(ob.lastPriceOfDay);
                sb.append(";").append(ob.numberOfPricesFixed).append(";");
                this.println("Day;" + nbDays + ";" + sb.toString());
            }
        }

    }

    public void tick(Day day, Collection<OrderBook> orderbooks) {
        if(this.pw != null) {
            System.out.println(day.currentTick() + " >> " + nb_order);
            nb_order = 0;
            tsb.setCurrentTick(day.currentTick());
            tsb.setTimeStamp(tsb.baseTimeStampForCurrentTick());

            Iterator<OrderBook> i$ = orderbooks.iterator();

            while(i$.hasNext()) {
                OrderBook ob = (OrderBook)i$.next();
                StringBuilder sb = new StringBuilder();
                sb.append("Tick;").append(day.currentPeriod().currentTick()).append(";");
                sb.append(ob.obName).append(";" + (ob.ask.size() > 0?Long.valueOf(((LimitOrder)ob.ask.first()).price):"0"));
                sb.append(";").append(ob.bid.size() > 0?Long.valueOf(((LimitOrder)ob.bid.first()).price):"0");
                sb.append(";").append(ob.lastFixedPrice != null?Long.valueOf(ob.lastFixedPrice.price):"0").append(";");
                this.println(sb.toString());
            }
        }

    }

    public void exec(Order o) {
        if(this.pw != null) {
            this.println("Exec;" + o.sender.name + "-" + o.extId);
        }

    }

    public void price(PriceRecord p, long bestAskPrice, long bestBidPrice) {
        if(this.pw != null) {
            this.println("Price;" + p + ";" + bestAskPrice + ";" + bestBidPrice);
        }

    }

    public void agent(Agent a, Order o, PriceRecord p) {
        if(this.pw != null) {
            this.println("Agent;" + a.name + ";" + a.cash + ";" + o.obName + ";" + a.getInvest(o.obName) + ";" + (p != null?Long.valueOf(p.price):"none"));
        }

    }

    public void command(char c) {
        if(this.pw != null) {
            this.println("!" + c);
        }

    }

    public void info(String s) {
        if(this.pw != null) {
            this.println("Info;" + s);
        }

    }

    /*private String displayTimestamp() {
        long ts = tsb.nextTimeStamp();
        return ";" + ts;
    }*/
}
