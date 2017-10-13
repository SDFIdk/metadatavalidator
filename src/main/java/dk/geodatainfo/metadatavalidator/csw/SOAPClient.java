package dk.geodatainfo.metadatavalidator.csw;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;

import javax.xml.soap.SOAPConnection;
import javax.xml.soap.SOAPConnectionFactory;
import javax.xml.soap.SOAPException;
import javax.xml.soap.SOAPMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SOAPClient {

	private static final Logger LOGGER = LoggerFactory.getLogger(SOAPClient.class);

	private SOAPConnectionFactory soapConnectionFactory;
	private SOAPConnection soapConnection;

	public SOAPClient() throws UnsupportedOperationException, SOAPException {
		soapConnectionFactory = SOAPConnectionFactory.newInstance();
	}

	public SOAPMessage sendSOAPMessageToURLEndpoint(SOAPMessage request, URL url) throws SOAPException, IOException {
		try {
			soapConnection = soapConnectionFactory.createConnection();
			logSOAPMessage("request", request);
			SOAPMessage reply = soapConnection.call(request, url);
			logSOAPMessage("reply", reply);
			return reply;
		} finally {
			if (soapConnection != null) {
				try {
					soapConnection.close();
				} catch (SOAPException e) {
					LOGGER.debug("Ignoring exception", e);
				}
			}
		}
	}

	private void logSOAPMessage(String description, SOAPMessage soapMessage) throws SOAPException, IOException {
		if (LOGGER.isDebugEnabled()) {
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			soapMessage.writeTo(os);
			LOGGER.debug(description + ": ");
			LOGGER.debug(System.lineSeparator() + os.toString("UTF-8"));
		}
	}

}
