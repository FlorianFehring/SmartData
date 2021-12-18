package de.fhbielefeld.smartdata.dyn;

import de.fhbielefeld.smartdata.exceptions.DynException;
import java.util.List;

/**
 * Abstract class for database connection implementations
 * Gives basic connection functionallity
 * 
 * @author Florian Fehring
 */
public interface Dyn extends AutoCloseable {
    
    /**
     * Creates a connection to the database
     * @throws de.fhbielefeld.smartdata.exceptions.DynException
     */
    public void connect() throws DynException;
    
    /**
     * Disconnect from the connection to the database
     * Database is returend to servers connection pool
     * @throws DynException 
     */
    public void close() throws DynException;
    
    /**
     * Get warning that occured while processing
     * 
     * @return List of warning messages
     */
    public List<String> getWarnings();
}
