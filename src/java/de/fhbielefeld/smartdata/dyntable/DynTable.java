package de.fhbielefeld.smartdata.dyntable;

import de.fhbielefeld.smartdata.dbo.Column;
import de.fhbielefeld.smartdata.dbo.Table;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Class for admin a table
 * 
 * @author Florian Fehring
 */
public abstract class DynTable {
    
    protected String schema;
    protected String name;
    protected Connection con;
    
    public DynTable(String schema, String name, Connection con) {
        this.schema = schema;
        this.name = name;
        this.con = con;
    }
    
    /**
     * Checks if the specified table exists
     * 
     * @return true if table exists, false otherwise
     * 
     * @throws DynException 
     */
    public abstract boolean exists() throws DynException;
    
    /**
     * Creates the table
     * 
     * @param table Table definition of the table to create
     * @return True if table was created, false if table allready exists
     * @throws de.fhbielefeld.smartdata.exceptions.DynException
     */
    public abstract boolean create(Table table) throws DynException;
    
    /**
     * Adds columns to the table.
     * 
     * @param columns Columns to add
     * @return true if all columns are created, false if all columns already exists
     * @throws DynException 
     */
    public abstract boolean addColumns(List<Column> columns) throws DynException;
    
    /**
     * Gets all available columns for this table
     * 
     * @return List of collumns
     * @throws de.fhbielefeld.smartdata.exceptions.DynException
     */
    public abstract Map<String,Column> getColumns() throws DynException;
    
    /**
     * Get informations about one column
     * 
     * @param name Name of the column
     * @return Column information or null if column not found
     * 
     * @throws DynException 
     */
    public abstract Column getColumn(String name) throws DynException;
    
    /**
     * Gets the collumns that are uses for identity
     * 
     * @return List of identifying columns
     * 
     * @throws DynException 
     */
    public abstract List<Column> getIdentityColumns() throws DynException;
    
    /**
     * Changes the name of a column
     * 
     * @param oldname Old column name
     * @param newname New columnname
     * 
     * @throws DynException 
     */
    public abstract void changeColumnName(String oldname, String newname) throws DynException;
    
    /**
     * Changes the srid of a column (only on geometry columns)
     * @param columns Column definitions with changed informations
     * @throws DynException 
     */
    public abstract void changeColumns(List<Column> columns) throws DynException;
    
    /**
     * Deletes the table
     * 
     * @throws de.fhbielefeld.smartdata.exceptions.DynException
     */
    public abstract void delete() throws DynException;
}
