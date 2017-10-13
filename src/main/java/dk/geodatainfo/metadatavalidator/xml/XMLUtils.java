package dk.geodatainfo.metadatavalidator.xml;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public final class XMLUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(XMLUtils.class);

	private XMLUtils() {
	}

	public synchronized static DocumentBuilder createNamespaceAwareNonValidatingDocumentBuilder()
			throws ParserConfigurationException {
		DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
		documentBuilderFactory.setNamespaceAware(true);
		documentBuilderFactory.setValidating(false);
		DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
		return documentBuilder;
	}

	public synchronized static Transformer createTransformer()
			throws TransformerConfigurationException, TransformerFactoryConfigurationError {
		Transformer transformer = TransformerFactory.newInstance().newTransformer();
		try {
			transformer.setOutputProperty(OutputKeys.STANDALONE, "yes");
			transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
		} catch (IllegalArgumentException e) {
			LOGGER.warn("Unsupported output property, ignoring it: " + e.getMessage());
		}
		return transformer;
	}

	private synchronized static XPath createXPath(NamespaceContext namespaceContext) {
		XPathFactory xPathFactory = XPathFactory.newInstance();
		XPath xPath = xPathFactory.newXPath();
		xPath.setNamespaceContext(namespaceContext);
		return xPath;
	}

	public static NodeList selectNodes(Document document, String xPathExpressionAsString,
			NamespaceContext namespaceContext) throws XPathExpressionException {
		XPathExpression xPathExpression = XMLUtils.createXPath(namespaceContext).compile(xPathExpressionAsString);
		NodeList nodes = (NodeList) xPathExpression.evaluate(document, XPathConstants.NODESET);
		return nodes;
	}

}
