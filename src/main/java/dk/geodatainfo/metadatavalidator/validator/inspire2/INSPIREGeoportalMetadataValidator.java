package dk.geodatainfo.metadatavalidator.validator.inspire2;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.ws.commons.schema.utils.NamespaceMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

import dk.geodatainfo.metadatavalidator.utils.Utils;
import dk.geodatainfo.metadatavalidator.validator.AbstractMetadataValidator;
import dk.geodatainfo.metadatavalidator.validator.exception.MetadataValidatorException;
import dk.geodatainfo.metadatavalidator.xml.XMLUtils;

public class INSPIREGeoportalMetadataValidator extends AbstractMetadataValidator {

	private static final String NS_INSPIRE_COMMON = "http://inspire.ec.europa.eu/schemas/common/1.0";
	private static final String HTTP_INSPIRE_EC_EUROPA_EU_SCHEMAS_GEOPORTAL_1_0 = "http://inspire.ec.europa.eu/schemas/geoportal/1.0";

	private static final Logger LOGGER = LoggerFactory.getLogger(INSPIREGeoportalMetadataValidator.class);

	private CloseableHttpClient httpClient;
	private PoolingHttpClientConnectionManager connectionManager;
	private NamespaceContext namespaceContext;

	public INSPIREGeoportalMetadataValidator(PropertiesConfiguration config) throws MetadataValidatorException {
		super(config);
		connectionManager = new PoolingHttpClientConnectionManager();
		httpClient = HttpClients.createMinimal(connectionManager);
		namespaceContext = createNamespaceContext();
	}

	@Override
	public void sendRequestToURLEndpointAndSaveResults(File file)
			throws MetadataValidatorException, ConfigurationException {
		Validate.notNull(file);
		ValidationResult validationResult = sendRequest(file);
		saveValidationResults(file, validationResult);
	}

	private void saveValidationResults(File file, ValidationResult validationResult)
			throws TransformerFactoryConfigurationError, ConfigurationException, MetadataValidatorException {
		if (validationResult == null) {
			LOGGER.error("No validation result found for " + file.getAbsolutePath());
		} else {
			StringReader stringReader = null;
			FileWriterWithEncoding fileWriterWithEncoding = null;
			try {
				String validationReport = validationResult.getValidationReport();
				Transformer transformer = XMLUtils.createTransformer();
				File outputDirectory = Utils.getDirFromConfig(config, "dir.validationresult",
						"The location of the directory that will contain the validation results must be provided");
				File outputFile = new File(outputDirectory, file.getName());
				LOGGER.info("Start writing result to " + outputFile.getAbsolutePath());
				stringReader = new StringReader(validationReport);
				StreamSource xmlSource = new StreamSource(stringReader);
				// known that it is UTF-8, therefore not retrieving from result
				fileWriterWithEncoding = new FileWriterWithEncoding(outputFile, "UTF-8");
				StreamResult streamResult = new StreamResult(fileWriterWithEncoding);
				transformer.transform(xmlSource, streamResult);
				LOGGER.info("Finished writing result to " + outputFile.getAbsolutePath());
			} catch (TransformerException e) {
				throw new MetadataValidatorException(e);
			} catch (IOException e) {
				throw new MetadataValidatorException(e);
			} finally {
				IOUtils.closeQuietly(stringReader);
				IOUtils.closeQuietly(fileWriterWithEncoding);
			}
		}
	}

	private ValidationResult sendRequest(File file) throws MetadataValidatorException, ConfigurationException {
		// TODO update to use ResponseHandler, see also ETFClient

		/*
		 * Documentation on http://inspire-geoportal.ec.europa.eu/validator2/html/
		 * usingaswebservice.html#tabs_main-3
		 */

		String endpoint = config.getString("validator.endpoint");
		if (endpoint == null) {
			throw new ConfigurationException("A validator endpoint must be provided");
		}

		HttpPost httpPost = new HttpPost(endpoint);
		httpPost.addHeader("Accept", "application/xml");

		// text/plain for machine-to-machine interaction, see documentation
		FileEntity fileEntity = new FileEntity(file, ContentType.create("text/plain", "UTF-8"));
		httpPost.setEntity(fileEntity);
		CloseableHttpResponse response = null;
		try {
			StopWatch stopWatch = new StopWatch();
			stopWatch.start();
			LOGGER.info("Sending request to " + endpoint + " for " + file.getName());
			response = httpClient.execute(httpPost);
			stopWatch.stop();
			String statusLineAsString = response.getStatusLine().toString();
			LOGGER.info("Finished processing of " + file.getName() + " in " + stopWatch.toString() + " with status "
					+ statusLineAsString);
			int statusCode = response.getStatusLine().getStatusCode();
			switch (statusCode) {
			case 201:
				List<Header> headers = Arrays.asList(response.getAllHeaders());
				// The Location header of the response header is set to the
				// URI where the validation result is already available
				Header locationHeader = Collections2.filter(headers, new Predicate<Header>() {

					@Override
					public boolean apply(Header header) {
						return "Location".equals(header.getName());
					}
				}).iterator().next();
				String validationResultLocation = locationHeader.getValue();
				LOGGER.info("Result of the validation present at: " + validationResultLocation);

				HttpEntity httpEntity = response.getEntity();
				if (httpEntity == null) {
					throw new MetadataValidatorException(
							"Expected a message entity in the HTTP response, but none was found");
				} else {
					String entityContentAsString = EntityUtils.toString(httpEntity, "UTF-8");
					return new ValidationResult(validationResultLocation, entityContentAsString);
				}
			case 400:
				throw new MetadataValidatorException("Something is wrong with the content sent to the server");
			case 500:
				throw new MetadataValidatorException("An exception occurred on the server, try again later");
			default:
				throw new MetadataValidatorException(statusLineAsString);
			}
		} catch (IOException e) {
			throw new MetadataValidatorException(e);
		} finally {
			httpPost.releaseConnection();
			if (response != null) {
				try {
					response.close();
				} catch (IOException e) {
					LOGGER.warn("Could not close the response, continuing anyways");
				}
			}
		}
	}

	@Override
	public File createReport() throws ConfigurationException, MetadataValidatorException {
		LOGGER.info("Creating report");
		File outputDirectory = Utils.getDirFromConfig(config, "dir.validationresult",
				"The location of the directory that contains the validation results must be provided");
		CSVPrinter csvPrinter = null;
		File report;
		try {
			Collection<File> files = FileUtils.listFiles(outputDirectory, new String[] { "xml" }, false);
			report = new File(outputDirectory, "report.csv");
			FileWriterWithEncoding fileWriter = new FileWriterWithEncoding(report, "UTF-8");
			csvPrinter = new CSVPrinter(fileWriter, CSVFormat.RFC4180);
			csvPrinter.printRecord("Name", "Number of resources", "Completeness Indicator", "Validation Report URL");
			for (File file : files) {
				LOGGER.info("Adding " + file.getName() + " to the report");
				createAndPrintRecord(csvPrinter, file);
			}
			LOGGER.info("Created " + report.getAbsolutePath());
			return report;
		} catch (IOException | XPathExpressionException e) {
			throw new MetadataValidatorException("Report could not be created", e);
		} finally {
			IOUtils.closeQuietly(csvPrinter);
		}

	}

	private void createAndPrintRecord(CSVPrinter csvPrinter, File file)
			throws MetadataValidatorException, IOException, XPathExpressionException {
		String fileNameWithoutExtension = getFileNameWithoutExtension(file);
		Document document = parseFile(file);
		Integer numberOfResources = getNumberOfResources(document);
		String completenessIndicator = getCompletenessIndicator(document);
		String validationReportURL = getValidationReportURL(document);
		csvPrinter.printRecord(fileNameWithoutExtension, numberOfResources, completenessIndicator, validationReportURL);
	}

	private Integer getNumberOfResources(Document document) throws XPathExpressionException {
		Integer numberOfResources;
		String xPathExpressionAsString = "/ns2:Resource/ns2:PullBatchReportResource/ns2:FoundResourcesCount";
		NodeList foundResourcesCountElements = XMLUtils.selectNodes(document, xPathExpressionAsString,
				namespaceContext);
		if (foundResourcesCountElements.getLength() == 0) {
			NodeList resourceReportResources = XMLUtils.selectNodes(document,
					"/ns2:Resource/ns2:ResourceReportResource", namespaceContext);
			if (resourceReportResources.getLength() == 1) {
				numberOfResources = 1;
			} else {
				numberOfResources = -1;
				LOGGER.warn("Unexpected structure in validation report");
			}
		} else {
			numberOfResources = Integer.valueOf(getElementTextContent(document, xPathExpressionAsString));
		}
		return numberOfResources;
	}

	private String getCompletenessIndicator(Document document) throws XPathExpressionException {
		String completenessIndicator = getElementTextContent(document, "/ns2:Resource/ns2:CompletenessIndicator");
		return completenessIndicator;
	}

	private String getElementTextContent(Document document, String xPathExpressionAsString)
			throws XPathExpressionException {
		String text;
		NodeList matchingElements = XMLUtils.selectNodes(document, xPathExpressionAsString, namespaceContext);
		if (matchingElements.getLength() == 1) {
			Node node = matchingElements.item(0);
			if (node instanceof Element) {
				Element element = (Element) node;
				text = element.getTextContent();
			} else {
				LOGGER.error("Expected " + xPathExpressionAsString + " to be one element");
				text = "error";
			}
		} else {
			LOGGER.error("Expected 1 node matching " + xPathExpressionAsString);
			text = "error";
		}
		return text;
	}

	private String getValidationReportURL(Document document) throws XPathExpressionException {
		String geoportalMetadataLocatorURL = getElementTextContent(document,
				"/ns2:Resource/ns2:GeoportalMetadataLocator/*[local-name()='URL' and namespace-uri()='"
						+ NS_INSPIRE_COMMON + "']");
		String absoluteURL = "http://inspire-geoportal.ec.europa.eu/resources" + geoportalMetadataLocatorURL;
		return absoluteURL;
	}

	private String getFileNameWithoutExtension(File file) {
		int lastIndexOfDot = file.getName().lastIndexOf(".");
		String fileNameWithoutExtension = file.getName().substring(0, lastIndexOfDot);
		return fileNameWithoutExtension;
	}

	private Document parseFile(File file) throws MetadataValidatorException {
		try {
			DocumentBuilder documentBuilder = XMLUtils.createNamespaceAwareNonValidatingDocumentBuilder();
			LOGGER.info("Starting parsing of " + file.getAbsolutePath());
			Document document = documentBuilder.parse(file);
			return document;
		} catch (ParserConfigurationException | SAXException | IOException e) {
			throw new MetadataValidatorException("Could not parse " + file.getAbsolutePath(), e);
		}
	}

	private NamespaceContext createNamespaceContext() {
		NamespaceMap namespaceMap = new NamespaceMap();
		namespaceMap.add(XMLConstants.DEFAULT_NS_PREFIX, NS_INSPIRE_COMMON);
		namespaceMap.add("ns2", HTTP_INSPIRE_EC_EUROPA_EU_SCHEMAS_GEOPORTAL_1_0);
		namespaceMap.add("xsi", XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI);
		return namespaceMap;
	}

	@Override
	public void shutDown() {
		IOUtils.closeQuietly(httpClient);
	}
}
