package dk.geodatainfo.metadatavalidator;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import dk.geodatainfo.metadatavalidator.csw.CSWClient;
import dk.geodatainfo.metadatavalidator.csw.CSWException;
import dk.geodatainfo.metadatavalidator.utils.Utils;
import dk.geodatainfo.metadatavalidator.validator.MetadataValidator;
import dk.geodatainfo.metadatavalidator.validator.etf.ETFClient;
import dk.geodatainfo.metadatavalidator.validator.exception.MetadataValidatorException;
import dk.geodatainfo.metadatavalidator.validator.inspire2.INSPIREGeoportalMetadataValidator;
import dk.geodatainfo.metadatavalidator.xml.MetadataHandler;

public class Main { // NOPMD

	private static final String OPTION_CONFIG_FILE = "c";

	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

	private CSWClient cswClient;
	private MetadataHandler metadataHandler;
	private MetadataValidator validator;
	private PropertiesConfiguration config;
	private ThreadFactory threadFactory;
	private ExecutorService executorService;
	private boolean getMetadataRecords;
	private boolean validateMetadataRecords;
	private boolean createReport;

	public static void main(String[] args) {
		new Main().run(args);
	}

	public void run(String... args) {
		threadFactory = new ThreadFactoryBuilder().setNameFormat("Request-%d").setDaemon(false).build();
		executorService = Executors.newFixedThreadPool(10, threadFactory);
		try {
			LOGGER.info("Starting application");

			File configurationFile = getConfigurationFileFromCommandLineArgs(args);

			Configurations configurations = new Configurations();
			config = configurations.properties(configurationFile);

			File dirGetRecords = Utils.getExistingDirFromConfig(config, "dir.getrecords",
					"The location of a directory containing GetRecords-files must be provided");

			cswClient = new CSWClient(config);
			getMetadataRecords = config.getBoolean("csw.getrecords", true);
			metadataHandler = new MetadataHandler(config);
			validateMetadataRecords = config.getBoolean("validator.validaterecords", true);
			createReport = config.getBoolean("validator.createreport", true);
			if (validateMetadataRecords || createReport) {
				String validatortype = config.getString("validator.type").toLowerCase();
				switch (validatortype) {
				case "inspire2":
					validator = new INSPIREGeoportalMetadataValidator(config);
					break;
				case "etf":
					validator = new ETFClient(config);
					break;
				default:
					throw new MetadataValidatorException(
							"Unknown validator type " + validatortype + " given in the configuration");
				}
			} else {
				LOGGER.info("No validator is needed, not creating one.");
			}

			if (getMetadataRecords) {
				Utils.createAndCleanDirectory(Utils.getDirFromConfig(config, "dir.getrecordsresponse",
						"The location of the directory that will contain the matching metadata must be provided"));
			}
			if (validateMetadataRecords) {
				Utils.createAndCleanDirectory(Utils.getDirFromConfig(config, "dir.validationresult",
						"The location of the directory that will contain the validation results must be provided"));
			}
			retrieveAndProcessMetadata(dirGetRecords);
			if (createReport) {
				validator.createReport();
			}
			LOGGER.info("Finished");
		} catch (ParseException e) {
			LOGGER.error("Incorrect command line argument given", e);
		} catch (ConfigurationException e) {
			LOGGER.error("There is an error with or in the configuration file", e);
		} catch (CSWException e) {
			LOGGER.error("Could not create the CWS client", e);
		} catch (MetadataValidatorException e) {
			LOGGER.error(e.getMessage(), e);
		} catch (ParserConfigurationException e) {
			LOGGER.error(e.getMessage(), e);
		} catch (TransformerException e) {
			LOGGER.error(e.getMessage(), e);
		} catch (InterruptedException e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			executorService.shutdown();
			if (validator != null) {
				validator.shutDown();
			}
		}
	}

	/**
	 * Retrieves and processes metadata, taking into account the configuration (retrieval and or validation may be
	 * disabled).
	 */
	private void retrieveAndProcessMetadata(File dirGetRecords) throws CSWException, ParserConfigurationException,
			ConfigurationException, TransformerException, InterruptedException {
		Collection<MetadataValidatorCallable> metadataValidatorcallables = new ArrayList<>();

		if (getMetadataRecords) {
			Validate.notNull(dirGetRecords);
			File[] files = dirGetRecords.listFiles();
			Validate.notNull(files);
			// TODO add XML validation of the files using an XML Catalog
			for (File file : files) {
				Document matchingRecords = cswClient.getMatchingRecords(file);
				if (matchingRecords == null) {
					LOGGER.info("No matching records found for " + file.getAbsolutePath());
				} else {
					metadataHandler.provideStatisticsForMetadata(matchingRecords, file.getName());
					File getRecordsResponseFile = metadataHandler.saveMetadataRecordsAsIs(matchingRecords,
							file.getName());
					// validation on the server may take some time, therefore using multithreading
					metadataValidatorcallables.add(new MetadataValidatorCallable(getRecordsResponseFile));
				}
			}
		} else { // validate metadata records on that already are in folder
			File dirMetadata = Utils.getExistingDirFromConfig(config, "dir.getrecordsresponse",
					"The location of a directory containing metadata must be provided");
			for (File file : dirMetadata.listFiles()) {
				// validation on the server may take some time, therefore using multithreading
				metadataValidatorcallables.add(new MetadataValidatorCallable(file));
			}
		}

		if (validateMetadataRecords) {
			executorService.invokeAll(metadataValidatorcallables);
		}
	}

	private File getConfigurationFileFromCommandLineArgs(String... args) throws ParseException {
		Options options = createAndPrintOptions();
		CommandLine commandLine = new DefaultParser().parse(options, args);
		File configurationFile = (File) commandLine.getParsedOptionValue(OPTION_CONFIG_FILE);
		return configurationFile;
	}

	private Options createAndPrintOptions() {
		Options options = new Options();
		options.addOption(Option.builder(OPTION_CONFIG_FILE).argName("file").desc("configuration properties file")
				.hasArg().numberOfArgs(1).required().type(File.class).build());
		HelpFormatter helpFormatter = new HelpFormatter();
		helpFormatter.printHelp("metadatavalidator", options);
		return options;
	}

	private class MetadataValidatorCallable implements Callable<Boolean> {

		private File getRecordsResponseFile;

		public MetadataValidatorCallable(File getRecordsResponseFile) {
			this.getRecordsResponseFile = getRecordsResponseFile;
		}

		@Override
		public Boolean call() {
			try {
				validator.sendRequestToURLEndpointAndSaveResults(getRecordsResponseFile);
				return Boolean.TRUE;
			} catch (MetadataValidatorException | ConfigurationException e) {
				LOGGER.error(e.getMessage(), e);
				return Boolean.FALSE;
			} catch (Exception e) {
				LOGGER.error("Error in thread", e);
				return Boolean.FALSE;
			}
		}

	}

}
