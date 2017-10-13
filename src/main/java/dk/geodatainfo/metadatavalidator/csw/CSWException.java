package dk.geodatainfo.metadatavalidator.csw;

public class CSWException extends Exception {

	private static final long serialVersionUID = 5738396550269653946L;

	public CSWException() {
		super();
	}

	public CSWException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public CSWException(String message, Throwable cause) {
		super(message, cause);
	}

	public CSWException(String message) {
		super(message);
	}

	public CSWException(Throwable cause) {
		super(cause);
	}
	
	

}
