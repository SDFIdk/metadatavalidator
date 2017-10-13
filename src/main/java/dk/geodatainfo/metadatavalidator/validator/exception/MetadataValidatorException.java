package dk.geodatainfo.metadatavalidator.validator.exception;

public class MetadataValidatorException extends Exception {

	private static final long serialVersionUID = 8451727676142454948L;

	public MetadataValidatorException() {
		super();
	}

	public MetadataValidatorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public MetadataValidatorException(String message, Throwable cause) {
		super(message, cause);
	}

	public MetadataValidatorException(String message) {
		super(message);
	}

	public MetadataValidatorException(Throwable cause) {
		super(cause);
	}
	
	

}
