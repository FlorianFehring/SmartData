package de.fhbielefeld.smartdata.dyncollection;

import de.fhbielefeld.smartdata.dbo.Attribute;
import de.fhbielefeld.smartdata.dbo.DataCollection;
import de.fhbielefeld.smartdata.dyn.Dyn;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.util.List;
import java.util.Map;

/**
 * Class for admin a table
 * 
 * @author Florian Fehring
 */
public interface DynCollection extends Dyn {
    
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
    public abstract boolean create(DataCollection table) throws DynException;
    
    /**
     * Adds attributes to the collection.
     * 
     * @param attributes Attributes to add
     * @return true if all attributes are created, false if all attributes already exists
     * @throws DynException 
     */
    public abstract boolean addAttributes(List<Attribute> attributes) throws DynException;
    
    /**
     * Delets attributes from the collection
     * 
     * @param attributes List of attributes with at least the name
     * @return true if all attributes where deleted, false if no attribute was deleted
     * @throws DynException 
     */
    public abstract boolean delAttributes(List<Attribute> attributes) throws DynException;
    
    /**
     * Gets all available attributes for this table
     * 
     * @return List of collumns
     * @throws de.fhbielefeld.smartdata.exceptions.DynException
     */
    public abstract Map<String,Attribute> getAttributes() throws DynException;
    
    /**
     * Get informations about one attribute
     * 
     * @param name Name of the attribute
     * @return Attribute information or null if attribute not found
     * 
     * @throws DynException 
     */
    public abstract Attribute getAttribute(String name) throws DynException;
    
    /**
     * Searches a reference attribute to the given collection
     * 
     * @param collection    Name of the collection the reference should point to
     * @return Referencing attribute or null if none found
     * @throws DynException 
     */
    public abstract Attribute getReferenceTo(String collection) throws DynException;
    
    /**
     * Gets the collumns that are uses for identity
     * 
     * @return List of identifying attributes
     * 
     * @throws DynException 
     */
    public abstract List<Attribute> getIdentityAttributes() throws DynException;
    
    /**
     * Gets the collumns that have a geometry type
     * 
     * @return List of geo attributes
     * 
     * @throws DynException 
     */
    public abstract List<Attribute> getGeoAttributes() throws DynException;
    
    /**
     * Changes the name of a attributes
     * 
     * @param oldname Old attributes name
     * @param newname New attributename
     * 
     * @throws DynException 
     */
    public abstract void changeAttributeName(String oldname, String newname) throws DynException;
    
    /**
     * Changes the srid of a attribute (only on geometry attributes)
     * @param attributes Attribute definitions with changed informations
     * @throws DynException 
     */
    public abstract void changeAttributes(List<Attribute> attributes) throws DynException;
    
    /**
     * Deletes the table
     * 
     * @throws de.fhbielefeld.smartdata.exceptions.DynException
     */
    public abstract void delete() throws DynException;
}
