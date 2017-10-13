package dk.geodatainfo.metadatavalidator.validator;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.StringUtils;

import dk.geodatainfo.metadatavalidator.validator.exception.MetadataValidatorException;

public abstract class AbstractMetadataValidator implements MetadataValidator {

	protected PropertiesConfiguration config;
	private final String endpoint;

	public AbstractMetadataValidator(PropertiesConfiguration config) throws MetadataValidatorException {
		super();
		this.config = config;
		if (this.config.getString("validator.endpoint") == null) {
			throw new MetadataValidatorException(new ConfigurationException("A validator endpoint must be provided"));
		}
		endpoint = StringUtils.removeEnd(this.config.getString("validator.endpoint"), "/");
	}

	protected synchronized String getEndpoint() {
		return endpoint;
	}

}
