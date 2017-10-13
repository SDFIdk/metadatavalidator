package dk.geodatainfo.metadatavalidator.validator;

import java.io.File;

import org.apache.commons.configuration2.ex.ConfigurationException;

import dk.geodatainfo.metadatavalidator.validator.exception.MetadataValidatorException;

public interface MetadataValidator {

	void sendRequestToURLEndpointAndSaveResults(File metadata)
			throws MetadataValidatorException, ConfigurationException;

	File createReport() throws ConfigurationException, MetadataValidatorException;

	void shutDown();

}
