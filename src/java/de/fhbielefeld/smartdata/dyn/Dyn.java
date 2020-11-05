package de.fhbielefeld.smartdata.dyn;

import de.fhbielefeld.smartdata.exceptions.DynException;
import java.util.List;

/**
 * Abstract class for database connection implementations
 * Gives basic connection functionallity
 * 
 * @author Florian Fehring
 */
public interface Dyn {
    
    /**
     * Creates a connection to the database
     * @throws de.fhbielefeld.smartdata.exceptions.DynException
     */
    public void connect() throws DynException;
    
    /**
     * Closes a connection to the database
     */
    public void disconnect();
    
    /**
     * Get warning that occured while processing
     * 
     * @return List of warning messages
     */
    public List<String> getWarnings();
}
