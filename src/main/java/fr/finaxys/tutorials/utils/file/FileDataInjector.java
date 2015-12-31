package fr.finaxys.tutorials.utils.file;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;

import v13.Day;
import v13.LimitOrder;
import v13.Order;
import v13.OrderBook;
import v13.PriceRecord;
import v13.agents.Agent;
import fr.finaxys.tutorials.utils.AgentReferentialLine;
import fr.finaxys.tutorials.utils.AtomDataInjector;
import fr.finaxys.tutorials.utils.HadoopTutorialException;

public class FileDataInjector implements AtomDataInjector {
	
	private static final java.util.logging.Logger LOGGER = java.util.logging.Logger
			.getLogger(FileDataInjector.class.getName());
	
    private PrintStream pw = null;

    private String filename;

    public FileDataInjector(String filename) {
        
    }
    
    public FileDataInjector(PrintStream pw) {
        this.pw = pw;
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

	@Override
	public void createOutput() throws HadoopTutorialException {
		try {
			LOGGER.log(Level.INFO, "Creating File output on :"+filename);
			if (this.pw == null) {
				if (this.filename != null) {
					this.pw = new PrintStream(filename);
				} else {
					 throw new HadoopTutorialException("No filename define");
				}
			}
        } catch (IOException e) {
        	 throw new HadoopTutorialException("Cannot init FileDataInjector", e);
 		}
	}

	@Override
	public void sendAgent(long ts, Agent a, Order o, PriceRecord pr)
			throws HadoopTutorialException {
		if(this.pw != null) {
            this.println("Agent;" + a.name + ";" + a.cash + ";" + o.obName + ";" + a.getInvest(o.obName) + ";" + (pr != null?Long.valueOf(pr.price):"none"));
        }
	}

	@Override
	public void sendPriceRecord(long ts, PriceRecord pr, long bestAskPrice,
			long bestBidPrice) throws HadoopTutorialException {
		if(this.pw != null) {
            this.println("Price;" + pr + ";" + bestAskPrice + ";" + bestBidPrice);
        }
	}

	@Override
	public void sendAgentReferential(long ts, List<AgentReferentialLine> referencial)
			throws HadoopTutorialException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendOrder(long ts, Order o) throws HadoopTutorialException {
		if(this.pw != null) {
            this.print(o.toString()/*+displayTimestamp()*/);
        }
	}

	@Override
	public void sendTick(long ts, Day day, Collection<OrderBook> orderbooks)
			throws HadoopTutorialException {
		if(this.pw != null) {

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

	@Override
	public void sendDay(long ts, int nbDays, Collection<OrderBook> orderbooks)
			throws HadoopTutorialException {
		if(this.pw != null) {
			
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

	@Override
	public void sendExec(long ts, Order o) throws HadoopTutorialException {
		if(this.pw != null) {
            this.println("Exec;" + o.sender.name + "-" + o.extId);
        }
	}

	@Override
	public void closeOutput() throws HadoopTutorialException {
		this.pw.close();
		this.pw = null;
	}

}
