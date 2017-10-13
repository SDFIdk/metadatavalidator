package dk.geodatainfo.metadatavalidator.validator.etf;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.LocalDateTime;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dk.geodatainfo.metadatavalidator.utils.Utils;
import dk.geodatainfo.metadatavalidator.validator.AbstractMetadataValidator;
import dk.geodatainfo.metadatavalidator.validator.exception.MetadataValidatorException;

public class ETFClient extends AbstractMetadataValidator {

	private static final Logger LOGGER = LoggerFactory.getLogger(ETFClient.class);

	private CloseableHttpClient httpClient;

	private File outputDirectory;

	public ETFClient(PropertiesConfiguration config) throws MetadataValidatorException {
		super(config);
		httpClient = HttpClients.createDefault();
		try {
			outputDirectory = Utils.getDirFromConfig(config, "dir.validationresult",
					"The location of the directory that will contain the validation results must be provided");
		} catch (ConfigurationException e) {
			throw new MetadataValidatorException(e);
		}
	}

	@Override
	public void sendRequestToURLEndpointAndSaveResults(File file)
			throws MetadataValidatorException, ConfigurationException {
		try {
			validateEndPointIsUpAndRunning();
			String testObjectId = uploadMetadata(file);
			String testRunId = startTestRun(file, testObjectId);
			waitForTestRunToFinish(testRunId);
			getAndSaveTestReport(file.getName(), testRunId);
		} catch (IOException e) {
			throw new MetadataValidatorException(e);
		}
	}

	public boolean validateEndPointIsUpAndRunning() throws ClientProtocolException, IOException {
		HttpHead httpHead = new HttpHead(getEndpoint() + "/v2/heartbeat");
		ResponseHandler<Boolean> responseHandler = new ResponseHandler<Boolean>() {

			@Override
			public Boolean handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
				StatusLine statusLine = response.getStatusLine();
				int statusCode = statusLine.getStatusCode();
				LOGGER.debug(statusCode + " - " + statusLine.getReasonPhrase());

				switch (statusCode) {
				case 200:
				case 204:
					return true;
				case 404:
				case 500:
				case 503:
					throw new ClientProtocolException("Service is down");
				default:
					throw new ClientProtocolException(
							"Unexpected response " + statusCode + " " + response.getStatusLine().getReasonPhrase());

				}
			}
		};
		return httpClient.execute(httpHead, responseHandler);
	}

	private String uploadMetadata(File file) throws MetadataValidatorException, ClientProtocolException, IOException {
		HttpPost postUploadTestObject = createPostTestObject(file);
		ResponseHandler<String> responseHandler = new ResponseHandler<String>() {

			@Override
			public String handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
				int statusCode = response.getStatusLine().getStatusCode();
				switch (statusCode) {
				case 200:
					LOGGER.info("File uploaded and temporary Test Object created");
					return getTestObjectId(response);
				case 400:
					throw new ClientProtocolException("File upload failed");
				case 413:
					throw new ClientProtocolException("Uploaded test data are too large");
				default:
					throw new ClientProtocolException(
							"Unexpected response " + statusCode + " " + response.getStatusLine().getReasonPhrase());
				}
			}

			private String getTestObjectId(HttpResponse response)
					throws IOException, JsonParseException, JsonMappingException {
				String jsonString = EntityUtils.toString(response.getEntity(), Charset.forName("UTF-8"));
				LOGGER.debug(jsonString);
				JsonNode node = new ObjectMapper().readValue(jsonString, JsonNode.class);
				String testObjectId = node.get("testObject").get("id").asText();
				LOGGER.debug("Test object id: " + testObjectId);
				return testObjectId;
			}
		};
		return httpClient.execute(postUploadTestObject, responseHandler);
	}

	private String startTestRun(File file, String testObjectId) throws IOException, ClientProtocolException {
		HttpPost postStartTestRun = createPostStartTestRun(file, testObjectId);
		ResponseHandler<String> responseHandler = new ResponseHandler<String>() {

			@Override
			public String handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
				int statusCode = response.getStatusLine().getStatusCode();
				String entityContent = EntityUtils.toString(response.getEntity(), Charset.forName("UTF-8"));
				switch (statusCode) {
				case 201:
					LOGGER.info("Test Run created");
					return getTestRunId(entityContent);
				case 200:
					throw new ClientProtocolException("Status 200 but no test run created");
				case 400:
					throw new ClientProtocolException("Invalid request: " + entityContent);
				case 404:
					throw new ClientProtocolException(
							"Test Object or Executable Test Suite with ID not found: " + entityContent);
				case 409:
					throw new ClientProtocolException("Test Object already in use: " + entityContent);
				case 500:
					throw new ClientProtocolException("Internal error: " + entityContent);
				default:
					throw new ClientProtocolException(
							"Unexpected response " + statusCode + " " + response.getStatusLine().getReasonPhrase());
				}
			}

			private String getTestRunId(String entityContent) throws IOException {
				LOGGER.debug(entityContent);
				JsonNode node = new ObjectMapper().readValue(entityContent, JsonNode.class);
				String testRunId = node.get("EtfItemCollection").get("testRuns").get("TestRun").get("id").asText();
				LOGGER.debug("Test run id: " + testRunId);
				return testRunId;
			}

		};
		return httpClient.execute(postStartTestRun, responseHandler);
	}

	private void waitForTestRunToFinish(String testRunId) throws ClientProtocolException, IOException {
		HttpGet getProgressStatus = new HttpGet(getEndpoint() + "/v2/TestRuns/" + testRunId + "/progress");
		ResponseHandler<Boolean> responseHandler = new ResponseHandler<Boolean>() {

			@Override
			public Boolean handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
				int statusCode = response.getStatusLine().getStatusCode();
				switch (statusCode) {
				case 200:
					String entityContent = EntityUtils.toString(response.getEntity(), Charset.forName("UTF-8"));
					LOGGER.debug(entityContent);
					JsonNode node = new ObjectMapper().readValue(entityContent, JsonNode.class);
					int max = node.get("max").asInt();
					int val = node.get("val").asInt();
					boolean isFinished = val < max ? false : true;
					return isFinished;
				case 404:
					throw new ClientProtocolException("Test Run not found");
				default:
					throw new ClientProtocolException(
							"Unexpected response " + statusCode + " " + response.getStatusLine().getReasonPhrase());
				}
			}
		};
		boolean testRunHasFinished = false;
		do {
			try {
				LOGGER.info("Waiting for test run to finish...");
				Thread.sleep(10 * 1000); // wait 10 seconds
				testRunHasFinished = httpClient.execute(getProgressStatus, responseHandler);
			} catch (InterruptedException e) {
				// https://www.ibm.com/developerworks/java/library/j-jtp05236/index.html
				Thread.currentThread().interrupt();
			}
		} while (!testRunHasFinished);
		LOGGER.info("Test run finished");
	}

	private HttpPost createPostStartTestRun(File file, String testObjectId) throws IOException {
		HttpPost postStartTestRun;
		postStartTestRun = new HttpPost(getEndpoint() + "/v2/TestRuns");
		postStartTestRun.addHeader("Accept", "application/json");
		postStartTestRun.addHeader("Content-Type", "application/json");
		HttpEntity entity;
		entity = EntityBuilder.create().setText(createRunRequestJson(file.getName(), testObjectId)).build();
		postStartTestRun.setEntity(entity);
		return postStartTestRun;
	}

	private String createRunRequestJson(String fileName, String testObjectId) throws IOException {
		String metadataTestSuiteID = "EID9a31ecfc-6673-43c0-9a31-b4595fb53a98";
		JsonFactory jsonFactory = new JsonFactory();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String runRequest = null;
		JsonGenerator generator = jsonFactory.createGenerator(out);
		generator.writeStartObject();
		generator.writeStringField("label", "Metadata validation - " + fileName + " - " + LocalDateTime.now());
		generator.writeArrayFieldStart("executableTestSuiteIds");
		generator.writeString(metadataTestSuiteID);
		generator.writeEndArray();
		generator.writeObjectFieldStart("arguments");
		generator.writeStringField("files_to_test", ".*");
		generator.writeStringField("tests_to_execute", ".*");
		generator.writeEndObject();
		generator.writeObjectFieldStart("testObject");
		generator.writeStringField("id", testObjectId);
		generator.writeEndObject();
		generator.writeEndObject();
		generator.close();
		runRequest = out.toString("UTF-8");
		LOGGER.debug(runRequest);
		return runRequest;
	}

	private HttpPost createPostTestObject(File file) {
		HttpPost postTestObject;
		postTestObject = new HttpPost(getEndpoint() + "/v2/TestObjects");
		postTestObject.addHeader("Accept", "application/json");
		FileBody fileBody = new FileBody(file, ContentType.create("application/xml", "UTF-8"));
		HttpEntity requestEntity = MultipartEntityBuilder.create().addTextBody("action", "upload")
				.addPart("fileupload", fileBody).build();
		postTestObject.setEntity(requestEntity);
		return postTestObject;
	}

	private void getAndSaveTestReport(String fileName, String testRunId) throws ClientProtocolException, IOException {
		HttpGet getTestReportHtml = new HttpGet(getEndpoint() + "/v2/TestRuns/" + testRunId + ".html?download=true");
		HttpGet getTestReportJson = new HttpGet(getEndpoint() + "/v2/TestRuns/" + testRunId + ".json?download=true");
		ResponseHandler<Boolean> responseHandler = new ResponseHandler<Boolean>() {

			@Override
			public Boolean handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
				int statusCode = response.getStatusLine().getStatusCode();
				switch (statusCode) {
				case 200:
				case 202:
					saveReport(response.getEntity());
					return true;
				case 404:
					throw new ClientProtocolException("Test Run does not exist");
				case 406:
					throw new ClientProtocolException("Test Run not finished yet");
				default:
					throw new ClientProtocolException(
							"Unexpected response " + statusCode + " " + response.getStatusLine().getReasonPhrase());
				}
			}

			private void saveReport(HttpEntity entity) throws ParseException, IOException {
				FileWriterWithEncoding fileWriterWithEncoding = null;
				String fileNameWithoutExtension = FilenameUtils.removeExtension(fileName);
				String fileNameWithCorrectExtension;
				try {
					Charset charset = ContentType.getOrDefault(entity).getCharset();
					String mimeType = ContentType.getOrDefault(entity).getMimeType();
					switch (mimeType) {
					case "text/xml":
					case "applicaton/xml":
						fileNameWithCorrectExtension = fileNameWithoutExtension + ".xml";
						break;
					case "text/html":
						fileNameWithCorrectExtension = fileNameWithoutExtension + ".html";
						break;
					case "application/json":
						fileNameWithCorrectExtension = fileNameWithoutExtension + ".json";
						break;
					default:
						throw new IllegalArgumentException("Unexpected mime type " + mimeType);

					}

					String entityContent = EntityUtils.toString(entity, charset);
					File outputFile = new File(outputDirectory, fileNameWithCorrectExtension);
					LOGGER.info("Start writing result to " + outputFile.getAbsolutePath());
					fileWriterWithEncoding = new FileWriterWithEncoding(outputFile, charset);
					fileWriterWithEncoding.write(entityContent);
					LOGGER.info("Finished writing result to " + outputFile.getAbsolutePath());
				} finally {
					IOUtils.closeQuietly(fileWriterWithEncoding);
				}

			}

		};
		httpClient.execute(getTestReportHtml, responseHandler);
		httpClient.execute(getTestReportJson, responseHandler);
	}

	@Override
	public File createReport() {
		throw new NotImplementedException("Creating a validation report is not yet implemented");
	}

	@Override
	public void shutDown() {
		IOUtils.closeQuietly(httpClient);
	}

}
