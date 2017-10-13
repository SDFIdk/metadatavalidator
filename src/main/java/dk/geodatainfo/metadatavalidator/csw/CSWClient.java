package dk.geodatainfo.metadatavalidator.csw;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.soap.MessageFactory;
import javax.xml.soap.SOAPConstants;
import javax.xml.soap.SOAPException;
import javax.xml.soap.SOAPMessage;
import javax.xml.soap.SOAPPart;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import dk.geodatainfo.metadatavalidator.xml.XMLUtils;

/**
 * Supports CSW 2.0.2.
 */
public class CSWClient {

	private static final String NS_CSW_2_0_2 = "http://www.opengis.net/cat/csw/2.0.2";
	private static final String NS_GMD = "http://www.isotc211.org/2005/gmd";
	private static final String GET_RECORDS = "GetRecords";
	public static final String GET_RECORDS_RESPONSE = "GetRecordsResponse";

	private static final Logger LOGGER = LoggerFactory.getLogger(CSWClient.class);

	private SOAPClient soapClient;
	private PropertiesConfiguration config;
	private URL endpoint;

	public CSWClient(PropertiesConfiguration config) throws CSWException, ConfigurationException {
		try {
			this.config = config;
			soapClient = new SOAPClient();
			endpoint = getCSWEndpointFromConfig();
		} catch (UnsupportedOperationException | SOAPException e) {
			throw new CSWException(e);
		}
	}

	/**
	 * @return document with a full result set, or null if no records are
	 *         matched // TODO reference
	 */
	public Document getMatchingRecords(File xmlFileGetRecordsOperation)
			throws CSWException, ParserConfigurationException, ConfigurationException {
		Document getRecordsFromInput = parseDocument(xmlFileGetRecordsOperation);
		int numberOfRecordsMatched = findNumberOfRecordsMatched(getRecordsFromInput);
		Document allMatchingRecords;
		if (numberOfRecordsMatched == 0) {
			allMatchingRecords = null;
		} else {
			allMatchingRecords = getAllMatchingRecords(getRecordsFromInput, numberOfRecordsMatched);
		}
		return allMatchingRecords;
	}

	private SOAPMessage createEmptyMessage() throws SOAPException, IOException {
		MessageFactory messageFactory = MessageFactory.newInstance(SOAPConstants.SOAP_1_2_PROTOCOL);
		SOAPMessage soapMessage = messageFactory.createMessage();
		return soapMessage;
	}

	private Document getAllMatchingRecords(Document getRecordsFromInput, int numberOfRecordsMatched)
			throws CSWException, ParserConfigurationException {
		List<Document> getRecordsReponses = getAllMatchingRecordsAsListOfRecordsResponses(getRecordsFromInput,
				numberOfRecordsMatched);
		if (getRecordsReponses.size() == 1) {
			return getRecordsReponses.get(0);
		} else {
			return mergeDocumentsToOne(numberOfRecordsMatched, getRecordsReponses);
		}
	}

	private Document mergeDocumentsToOne(int numberOfRecordsMatched, List<Document> getRecordsReponses)
			throws ParserConfigurationException {
		Document mergedRecordsResponse = XMLUtils.createNamespaceAwareNonValidatingDocumentBuilder().newDocument();
		Element elementGetRecordsResponse = getRecordsReponses.get(0).getDocumentElement();
		Element importedGetRecordsResponse = (Element) mergedRecordsResponse.importNode(elementGetRecordsResponse,
				true);
		Element elementSearchResults = (Element) importedGetRecordsResponse
				.getElementsByTagNameNS(NS_CSW_2_0_2, "SearchResults").item(0);
		elementSearchResults.setAttribute("nextRecord", "0");
		elementSearchResults.setAttribute("numberOfRecordsMatched", Integer.toString(numberOfRecordsMatched));
		elementSearchResults.setAttribute("numberOfRecordsReturned", Integer.toString(numberOfRecordsMatched));
		mergedRecordsResponse.appendChild(importedGetRecordsResponse);

		for (int i = 1; i < getRecordsReponses.size(); i++) {
			NodeList metadataElements = getRecordsReponses.get(i).getDocumentElement().getElementsByTagNameNS(NS_GMD,
					"MD_Metadata");
			for (int j = 0; j < metadataElements.getLength(); j++) {
				Node importedMetadataElement = mergedRecordsResponse.importNode(metadataElements.item(j), true);
				elementSearchResults.appendChild(importedMetadataElement);
			}
		}

		return mergedRecordsResponse;
	}

	private List<Document> getAllMatchingRecordsAsListOfRecordsResponses(Document getRecordsFromInput,
			int numberOfRecordsMatched) throws CSWException, ParserConfigurationException {
		int maxRecordsPerRequest = 100; // TODO make configurable
		int numberOfIterations = (int) Math.ceil((double) numberOfRecordsMatched / (double) maxRecordsPerRequest);
		List<Document> getRecordsReponses = new ArrayList<Document>(numberOfIterations);
		for (int i = 0; i < numberOfIterations; i++) {
			int startPosition = 1 + i * maxRecordsPerRequest;
			LOGGER.info("Retrieving records with startPosition " + startPosition);
			Document getRecordsFullResultset = createGetRecordsToRetrieveFullResultSet(getRecordsFromInput,
					startPosition, maxRecordsPerRequest);
			Document getRecordsResponse = getGetRecordsResponse(getRecordsFullResultset);
			getRecordsReponses.add(getRecordsResponse);
		}
		return getRecordsReponses;
	}

	private int findNumberOfRecordsMatched(Document getRecordsFromInput)
			throws CSWException, ParserConfigurationException {
		Document getRecordsToFindNumberOfRecordsMatched = createGetRecordsToFindNumberOfRecordsMatched(
				getRecordsFromInput);
		Document getRecordsResponse = getGetRecordsResponse(getRecordsToFindNumberOfRecordsMatched);
		int numberOfRecordsMatched = getNumberOfRecordsMatched(getRecordsResponse);
		return numberOfRecordsMatched;
	}

	private Document getGetRecordsResponse(Document document) throws CSWException, ParserConfigurationException {
		try {
			if (!GET_RECORDS.equals(document.getDocumentElement().getLocalName())) {
				throw new CSWException("The given document does not contain an operation with name " + GET_RECORDS);
			}
			SOAPMessage request = buildGetRecordsMessage(document);
			SOAPMessage reply = soapClient.sendSOAPMessageToURLEndpoint(request, endpoint);
			Document contentAsDocument = reply.getSOAPBody().extractContentAsDocument();
			if (!GET_RECORDS_RESPONSE.equals(contentAsDocument.getDocumentElement().getLocalName())) {
				throw new CSWException(
						"The reply from the server does not contain an element with name " + GET_RECORDS_RESPONSE);
			}
			LOGGER.info("Succesfully sent request to " + endpoint.toString());
			return contentAsDocument;
		} catch (SOAPException | SAXException | IOException | URISyntaxException e) {
			throw new CSWException("Operation " + GET_RECORDS + " failed, see the stacktrace for more information", e);
		}
	}

	private Document createGetRecordsToFindNumberOfRecordsMatched(Document getRecords)
			throws ParserConfigurationException {
		Document newDocument = XMLUtils.createNamespaceAwareNonValidatingDocumentBuilder().newDocument();
		Element elementGetRecords = getRecords.getDocumentElement();
		Element newElementGetRecords = (Element) newDocument.importNode(elementGetRecords, true);
		newElementGetRecords.setAttribute("resultType", "hits");
		newDocument.appendChild(newElementGetRecords);
		return newDocument;
	}

	private Document createGetRecordsToRetrieveFullResultSet(Document getRecords, int startPosition, int maxRecords)
			throws ParserConfigurationException {
		Document newDocument = XMLUtils.createNamespaceAwareNonValidatingDocumentBuilder().newDocument();
		Element elementGetRecords = getRecords.getDocumentElement();
		Element newElementGetRecords = (Element) newDocument.importNode(elementGetRecords, true);
		newElementGetRecords.setAttribute("resultType", "results");
		newElementGetRecords.setAttribute("startPosition", Integer.toString(startPosition));
		newElementGetRecords.setAttribute("maxRecords", Integer.toString(maxRecords));
		Element elementSetNameElement = (Element) newElementGetRecords
				.getElementsByTagNameNS(NS_CSW_2_0_2, "ElementSetName").item(0);
		elementSetNameElement.setTextContent("full");
		newDocument.appendChild(newElementGetRecords);
		return newDocument;
	}

	private int getNumberOfRecordsMatched(Document getRecordsResponse) {
		Element elementSearchResults = (Element) getRecordsResponse.getDocumentElement()
				.getElementsByTagNameNS(NS_CSW_2_0_2, "SearchResults").item(0);
		int numberOfRecordsMatched = Integer.parseInt(elementSearchResults.getAttribute("numberOfRecordsMatched"));
		LOGGER.info("numberOfRecordsMatched: " + numberOfRecordsMatched);
		return numberOfRecordsMatched;
	}

	private SOAPMessage buildGetRecordsMessage(Document document)
			throws SAXException, IOException, ParserConfigurationException, SOAPException, URISyntaxException {
		SOAPMessage soapMessage = createEmptyMessage();
		setGetRecordsMimeHeaders(soapMessage);
		soapMessage.getSOAPBody().addDocument(document);
		return soapMessage;
	}

	private Document parseDocument(File xmlFile) throws CSWException {
		try {
			DocumentBuilder documentBuilder = XMLUtils.createNamespaceAwareNonValidatingDocumentBuilder();
			LOGGER.info("Starting parsing of " + xmlFile.getAbsolutePath());
			Document document = documentBuilder.parse(xmlFile);
			return document;
		} catch (ParserConfigurationException | SAXException | IOException e) {
			throw new CSWException(xmlFile.getAbsolutePath() + " could not be parsed", e);
		}
	}

	private void setGetRecordsMimeHeaders(SOAPMessage soapMessage) {
		SOAPPart soapPart = soapMessage.getSOAPPart();
		soapPart.addMimeHeader("Accept-Encoding", "gzip,deflate");
		soapPart.addMimeHeader("Content-Type",
				"application/soap+xml;charset=UTF-8;action=\"http://inspire.jrc.ec.europa.eu/Discovery/GetRecords\"");
	}

	private URL getCSWEndpointFromConfig() throws ConfigurationException {
		endpoint = config.get(URL.class, "csw.endpoint");
		if (endpoint == null) {
			throw new ConfigurationException("A CSW url endpoint must be provided.");
		}
		return endpoint;
	}

}
