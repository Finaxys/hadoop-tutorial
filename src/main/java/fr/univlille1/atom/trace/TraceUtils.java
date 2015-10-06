package fr.univlille1.atom.trace;

import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TraceUtils {
	
	private static final Logger LOGGER = Logger.getLogger(TraceUtils.class.getName());
	
	public static final boolean isTrace(TraceType type, String trace) {
		if (trace == null) return false;
		return type.equals(lookupType(trace));
	}
	
	public static final TraceType lookupType(String trace) {
		if (trace == null || "".equals(trace.trim())) return null;
		char c = trace.charAt(0);
		LOGGER.log(Level.FINEST,"lookupType:" + c);
		TraceType type;
		switch (c) {
			case 'O' : type = TraceType.Order; break;
			case 'P' : type = TraceType.Price; break;
			case 'E' : type = TraceType.Exec; break;
			case 'A' : type = TraceType.Agent; break;
			case 'D' : type = TraceType.Day; break;
			case 'T' : type = TraceType.Tick; break;
			default: type = null; break;
		}
		return type; 
	}
	
	public static final Trace buildTrace(String trace) {
		
		TraceType type = lookupType(trace);
		if (type == null) return null;
		LOGGER.log(Level.FINE,"buildTrace:" + trace);
		
		Trace t = null;
		StringTokenizer st = new StringTokenizer(trace, ";");
		st.nextToken(); // dropping line header
		
		if (TraceType.Agent.equals(type)) {
			t = new AgentTrace(st.nextToken(), Long.parseLong(st.nextToken()), st.nextToken(),
					Long.parseLong(st.nextToken()), Long.parseLong(st.nextToken()));
		} else if (TraceType.Day.equals(type)) {
			t = new DayTrace(Long.parseLong(st.nextToken()), st.nextToken(),
					Long.parseLong(st.nextToken()), Long.parseLong(st.nextToken()),
					Long.parseLong(st.nextToken()), Long.parseLong(st.nextToken()),
					Long.parseLong(st.nextToken()));
		} else if (TraceType.Exec.equals(type)) {
			t = new ExecTrace(st.nextToken());
		} else if (TraceType.Order.equals(type)) {
			t = new OrderTrace(st.nextToken(), st.nextToken(), st.nextToken(),
					OrderNature.valueOf(st.nextToken()), OrderDirection.valueOf(st.nextToken()),
					Long.parseLong(st.nextToken()), Long.parseLong(st.nextToken()),
					Integer.parseInt(st.nextToken())/*, Long.parseLong(st.nextToken())*/);
		} else if (TraceType.Price.equals(type)) {
			t = new PriceTrace(st.nextToken(), Long.parseLong(st.nextToken()),
					Long.parseLong(st.nextToken()),
					OrderDirection.valueOf(st.nextToken()), st.nextToken(),
					st.nextToken(), Long.parseLong(st.nextToken()),
					Long.parseLong(st.nextToken()));
		} else if (TraceType.Tick.equals(type)) {
			t = new TickTrace(Long.parseLong(st.nextToken()), st.nextToken(),
					Long.parseLong(st.nextToken()), Long.parseLong(st.nextToken()),
					Long.parseLong(st.nextToken()));
		}
		return t;
	}

}
