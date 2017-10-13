package dk.geodatainfo.metadatavalidator.xml;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.Validate;
import org.apache.ws.commons.schema.utils.NamespaceMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;

import dk.geodatainfo.metadatavalidator.csw.CSWClient;
import dk.geodatainfo.metadatavalidator.utils.Utils;

public class MetadataHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(MetadataHandler.class);

	private static final String GCO_NAMESPACE = "http://www.isotc211.org/2005/gco";
	private static final String GMD_NAMESPACE = "http://www.isotc211.org/2005/gmd";

	private PropertiesConfiguration config;
	private NamespaceContext namespaceContext;
	private Map<Queryable, String> queryablemap;

	private enum Queryable {
		/**
		 * MetadataPointOfContact, defined in AdditionalQueryables (INSPIRE community)
		 */
		METADATA_POINT_OF_CONTACT,
		/**
		 * OrganisationName, defined in SupportedISOQueryables
		 */
		ORGANISATION_NAME;
	}

	public MetadataHandler(PropertiesConfiguration config) {
		namespaceContext = createNamespaceContext();
		queryablemap = createQueryablemap();
		this.config = config;
	}

	private NamespaceContext createNamespaceContext() {
		NamespaceMap namespaceMap = new NamespaceMap();
		namespaceMap.add("xsi", XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI);
		namespaceMap.add("gmd", GMD_NAMESPACE);
		namespaceMap.add("gco", GCO_NAMESPACE);
		namespaceMap.add("csw", "http://www.opengis.net/cat/csw/2.0.2");
		namespaceMap.add("srv", "http://www.isotc211.org/2005/srv");
		return namespaceMap;
	}

	private Map<Queryable, String> createQueryablemap() {
		HashMap<Queryable, String> queryablemap = new HashMap<>();
		/*
		 * based on info in Technical Guidance for the implementation of INSPIRE
		 * Discovery Services, v3.1, p. 20, and on Github in
		 * core-geonetwork/web/src/main/webapp/WEB-INF/config-csw.xml
		 */
		queryablemap.put(Queryable.METADATA_POINT_OF_CONTACT,
				"//gmd:MD_Metadata/gmd:contact/gmd:CI_ResponsibleParty/gmd:organisationName/gco:CharacterString");
		// based on info in OGC 07-045, p. 47
		queryablemap.put(Queryable.ORGANISATION_NAME,
				"//gmd:MD_Metadata/gmd:identificationInfo//gmd:pointOfContact/gmd:CI_ResponsibleParty/gmd:organisationName/gco:CharacterString");
		return queryablemap;
	}

	/**
	 * @return directory that contains the metadata records as XML files with
	 *         MD_Metadata as root element
	 */
	public void saveMetadataRecordsAsSeparateFiles(Document document) throws ParserConfigurationException,
			TransformerFactoryConfigurationError, TransformerException, ConfigurationException {
		validateDocument(document);
		saveMetadataRecords(extractMetadataRecords(document));
	}

	public void provideStatisticsForMetadata(Document document, String fileName) {
		try {
			Queryable queryable = Queryable.METADATA_POINT_OF_CONTACT;
			Multiset<String> summary = ConcurrentHashMultiset.create();
			NodeList nodes = XMLUtils.selectNodes(document, queryablemap.get(queryable), namespaceContext);
			for (int i = 0; i < nodes.getLength(); i++) {
				summary.add(nodes.item(i).getTextContent());
			}

			LOGGER.info("----------");
			LOGGER.info("Statistics for " + fileName + ", grouped by " + queryable + " (" + summary.size() + ")");
			LOGGER.info("----------");
			for (String string : summary.elementSet()) {
				LOGGER.info("\t" + string + " (" + summary.count(string) + ")");
			}
		} catch (XPathExpressionException e) {
			LOGGER.warn("Logging failed", e);
		}
		LOGGER.info("----------");
	}

	/**
	 * @return XML file that contains MD_Metadata elements, nested in root element
	 *         GetRecordsResponse
	 */
	public File saveMetadataRecordsAsIs(Document document, String fileName)
			throws TransformerException, ParserConfigurationException, ConfigurationException {
		validateDocument(document);
		Transformer transformer = XMLUtils.createTransformer();
		File directory = Utils.getDirFromConfig(config, "dir.getrecordsresponse",
				"The location of the directory that will contain the matching metadata must be provided");
		File savedGetRecordsResponse = saveDocumentToFile(document, directory, fileName, transformer);
		return savedGetRecordsResponse;
	}

	private void validateDocument(Document document) {
		Validate.isTrue(CSWClient.GET_RECORDS_RESPONSE.equals(document.getDocumentElement().getLocalName()),
				"Document must have root element " + CSWClient.GET_RECORDS_RESPONSE, document);
	}

	/**
	 * @return NodeList of Elements
	 */
	private NodeList extractMetadataRecords(Document metadataRecordsDocument) {
		NodeList metadataRecords = metadataRecordsDocument.getDocumentElement().getElementsByTagNameNS(GMD_NAMESPACE,
				"MD_Metadata");
		LOGGER.info(metadataRecords.getLength() + " metadata records found");
		return metadataRecords;
	}

	private void saveMetadataRecords(NodeList nodeList) throws ParserConfigurationException,
			TransformerFactoryConfigurationError, TransformerException, ConfigurationException {
		DocumentBuilder documentBuilder = XMLUtils.createNamespaceAwareNonValidatingDocumentBuilder();
		Transformer transformer = XMLUtils.createTransformer();
		File directory = Utils.getDirFromConfig(config, "dir.getrecordsresponse",
				"The location of the directory that will contain the matching metadata must be provided");

		for (int i = 0; i < nodeList.getLength(); i++) {
			Element element = (Element) nodeList.item(i);
			saveElementToFile(documentBuilder, transformer, directory, element);
		}
	}

	private void saveElementToFile(DocumentBuilder documentBuilder, Transformer transformer, File directory,
			Element element) throws TransformerException {
		Document document = createDocument(documentBuilder, element);
		String metadataID = findMetadataID(element);
		saveDocumentToFile(document, directory, metadataID, transformer);
	}

	private File saveDocumentToFile(Document document, File directory, String fileName, Transformer transformer)
			throws TransformerException {
		File metadataFile = new File(directory, fileName);
		transformer.transform(new DOMSource(document), new StreamResult(metadataFile));
		LOGGER.info("Saved " + metadataFile.getAbsolutePath());
		return metadataFile;
	}

	private String findMetadataID(Element element) {
		Element fileIdentifierElement = (Element) element.getElementsByTagNameNS(GMD_NAMESPACE, "fileIdentifier")
				.item(0);
		Element characterStringElement = (Element) fileIdentifierElement
				.getElementsByTagNameNS(GCO_NAMESPACE, "CharacterString").item(0);
		String metadataID = characterStringElement.getTextContent();
		return metadataID;
	}

	private Document createDocument(DocumentBuilder documentBuilder, Element element) {
		Document document = documentBuilder.newDocument();
		Node importedNode = document.importNode(element, true);
		document.appendChild(importedNode);
		return document;
	}

}
