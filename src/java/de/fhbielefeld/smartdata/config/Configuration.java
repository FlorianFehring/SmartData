package de.fhbielefeld.smartdata.config;

import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.scl.logger.Logger;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import javax.naming.NamingException;

/**
 * Class for reading the propierties format based configuration file
 *
 * @author Florian Fehring
 */
public class Configuration {

    private String moduleName;
    private String fileName;
    private boolean proploaded = false;
    private Properties prop;

    public Configuration() {
        this.prop = new Properties();
        try {
            this.moduleName = (String) new javax.naming.InitialContext().lookup("java:module/ModuleName");
            this.fileName = this.moduleName + "_config.properties";
            try ( InputStream inputStream = new FileInputStream(fileName)) {
                // Loading the properties.
                this.prop.load(inputStream);
                this.proploaded = true;
            } catch (IOException ex) {
                String workingDir = new File(".").getAbsolutePath();
                Message msg = new Message("Configuration", MessageLevel.ERROR, "Could not load properties file >" + workingDir + "/" + fileName + "<: " + ex.getLocalizedMessage());
                Logger.addMessage(msg);
            }
        } catch (NamingException ex) {
            Message msg = new Message("Configuration", MessageLevel.ERROR, "Could not load properties file" + ex.getLocalizedMessage());
            Logger.addMessage(msg);
        }
    }

    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public boolean isPropsloaded() {
        return proploaded;
    }

    /**
     * Gets the properties value
     *
     * @param name Name of the property
     * @return properites value
     */
    public String getProperty(String name) {
        return this.prop.getProperty(name);
    }

    public Set<Entry<Object, Object>> getAllProperties() {
        return this.prop.entrySet();
    }
}
