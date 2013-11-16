package org.opendedup.logging;

import java.io.IOException;

import java.io.StringWriter;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

public class JSONVolPerfLayout extends Layout {

	private final JsonFactory jsonFactory;
	private String[] mdcKeys = { "bytesRead", "bytesWritten",
			"virtualBytesWritten", "RIOPS", "WIOPS", "duplicateBytes" };

	public JSONVolPerfLayout() {
		jsonFactory = new JsonFactory();
	}

	@Override
	public String format(LoggingEvent event) {
		try {
			StringWriter stringWriter = new StringWriter();
			JsonGenerator g = createJsonGenerator(stringWriter);
			g.writeStartObject();
			writeBasicFields(event, g);
			writeMDCValues(event, g);
			writeThrowableEvents(event, g);
			writeNDCValues(event, g);
			g.writeEndObject();
			g.close();
			stringWriter.append("\n");
			return stringWriter.toString();
		} catch (IOException e) {
			throw new JSONLayoutException(e);
		}
	}

	private JsonGenerator createJsonGenerator(StringWriter stringWriter)
			throws IOException {
		JsonGenerator g = jsonFactory.createJsonGenerator(stringWriter);
		return g;
	}

	private void writeBasicFields(LoggingEvent event, JsonGenerator g)
			throws IOException {
		g.writeNumberField("timestamp", event.timeStamp);
		g.writeStringField("volume", event.getMessage().toString());
	}

	private void writeNDCValues(LoggingEvent event, JsonGenerator g)
			throws IOException {
		if (event.getNDC() != null) {
			g.writeStringField("NDC", event.getNDC());
		}
	}

	private void writeThrowableEvents(LoggingEvent event, JsonGenerator g)
			throws IOException {
		String throwableString;
		String[] throwableStrRep = event.getThrowableStrRep();
		throwableString = "";
		if (throwableStrRep != null) {
			for (String s : throwableStrRep) {
				throwableString += s + "\n";
			}
		}
		if (throwableString.length() > 0) {
			g.writeStringField("throwable", throwableString);
		}
	}

	private void writeMDCValues(LoggingEvent event, JsonGenerator g)
			throws IOException {
		if (mdcKeys.length > 0) {
			event.getMDCCopy();

			// g.writeObjectFieldStart("MDC");
			for (String s : mdcKeys) {
				Object mdc = event.getMDC(s);
				if (mdc != null) {
					g.writeStringField(s, mdc.toString());
				}
			}
			// g.writeEndObject();
		}
	}

	@Override
	public boolean ignoresThrowable() {
		return false;
	}

	@Override
	public void activateOptions() {
	}
}
