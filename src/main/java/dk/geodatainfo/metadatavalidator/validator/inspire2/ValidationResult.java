package dk.geodatainfo.metadatavalidator.validator.inspire2;

class ValidationResult {
	
	private String validationResultLocation;
	private String validationReport;
	
	public ValidationResult(String validationResultLocation, String validationReport) {
		super();
		this.validationResultLocation = validationResultLocation;
		this.validationReport = validationReport;
	}

	public String getValidationResultLocation() {
		return validationResultLocation;
	}

	public String getValidationReport() {
		return validationReport;
	}

}
