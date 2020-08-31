package de.fhbielefeld.smartdata.dynbase;

import de.fhbielefeld.smartdata.dbo.Table;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Methods for getting informations and createing global structures to a
 * dynamic base.
 * 
 * @author Florian Fehring
 */
public abstract class DynBase {
    
    /**
     * Gets a list of abilities that is supported by this DynBase implementation
     * 
     * @return Names of the abilities.
     * @throws DynException 
     */
    public abstract Collection<String> getAbilities() throws DynException;
    
    /**
     * Gets informations about an ability of the DynBase
     * 
     * @param abilityName Name of the ability to get information for
     * @return Key value pairs of information about the ability
     * @throws DynException Thrown on fetch error
     */
    public abstract Map<String,Object> getAbility(String abilityName) throws DynException;
    
    /**
     * Creates the ability indentified by the given name if possible.
     * 
     * @param abilityName Name of the ability
     * @return true if the ability was created, false if nothing was todo
     * @throws DynException Thrown on fetch error
     */
    public abstract boolean createAbilityIfNotExists(String abilityName) throws DynException;
    
    /**
     * Creates all abilities that do not exists and given in the collection
     * 
     * @param abilityNames Names of abilities
     * @return true if at least one ability was created, false if nothing was todo
     * @throws DynException Thrown on ability creation error
     */
    public boolean createAbilitiesIfNotExists(Collection<String> abilityNames) throws DynException {
        boolean created = false;
        for(String abilityName : abilityNames) {
            if(this.createAbilityIfNotExists(abilityName)) {
                created = true;
            }
        }
        return created;
    }
    
    /**
     * Creates a schema if it is not existend
     *
     * @param schemaName Name of the schema to create
     * @return true if schema was created, false if nothing was todo
     * @throws DynException Thrown on fetch error
     */
    public abstract boolean createSchemaIfNotExists(String schemaName) throws DynException;
    
    /**
     * Checks if a schema exists (and is accessible over the connection)
     * 
     * @param schemaName Name of the schema to search
     * @return true if the schema exists, false otherwise
     * 
     * @throws DynException 
     */
    public abstract boolean schemaExists(String schemaName) throws DynException;
    
    /**
     * Creates all schemata that do not exists and given in the collection
     * 
     * @param schemataNames Collection of schema names
     * @return true if at least one schema was created, false if nothing was todo
     * @throws DynException Thrown on creation error
     */
    public boolean createSchemaIfNotExists(Collection<String> schemataNames) throws DynException {
        boolean created = false;
        for(String schemaName : schemataNames) {
            if(this.createSchemaIfNotExists(schemaName)) {
                created = true;
            }
        }
        return created;
    }
    
    /**
     * Gets information about the schema with the given name
     * 
     * @param schemaName Name of the schema where informations are requested
     * @return Key-value pairs with information about the schema
     * @throws DynException Thrown on fetch error
     */
    public abstract Map<String,Object> getSchema(String schemaName) throws DynException;
    
    /**
     * Returns a list of available tables in the given schema
     * 
     * @param schemaName Name of the schema
     * @return List of table names
     * @throws DynException 
     */
    public abstract List<Table> getTables(String schemaName) throws DynException;
}
