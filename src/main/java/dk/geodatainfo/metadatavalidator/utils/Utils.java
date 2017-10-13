package dk.geodatainfo.metadatavalidator.utils;

import java.io.File;
import java.io.IOException;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Utils {

	private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

	private Utils() {
	}

	public static File createAndCleanDirectory(File directory) {
		if (!directory.exists()) {
			directory.mkdir();
		} else {
			try {
				FileUtils.cleanDirectory(directory);
			} catch (IOException e) {
				LOGGER.warn("Could not clean " + directory);
			}
		}
		return directory;
	}

	public static File getDirFromConfig(PropertiesConfiguration config, String key, String messageMissingKey)
			throws ConfigurationException {
		String dirLocation = config.getString(key);
		if (StringUtils.isBlank(dirLocation)) {
			throw new ConfigurationException(messageMissingKey + " in property with key " + key);
		}
		File dir = new File(dirLocation);
		if (dir.exists() && !dir.isDirectory()) {
			throw new ConfigurationException(dir.getAbsolutePath() + " must be a directory");
		}
		return dir;
	}

	public static File getExistingDirFromConfig(PropertiesConfiguration config, String key, String messageMissingKey)
			throws ConfigurationException {
		File dir = getDirFromConfig(config, key, messageMissingKey);
		if (!dir.exists()) {
			throw new ConfigurationException(dir.getAbsolutePath() + " does not exist");
		}
		return dir;
	}

}
