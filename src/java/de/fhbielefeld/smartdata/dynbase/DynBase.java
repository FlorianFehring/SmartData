package de.fhbielefeld.smartdata.dynbase;

import de.fhbielefeld.smartdata.dbo.DataCollection;
import de.fhbielefeld.smartdata.dyn.Dyn;
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
public interface DynBase extends Dyn {
    
    /**
     * Gets a list of abilities that is supported by this DynBase implementation
     * 
     * @return Names of the abilities.
     * @throws DynException 
     */
    public Collection<String> getAbilities() throws DynException;
    
    /**
     * Gets informations about an ability of the DynBase
     * 
     * @param abilityName Name of the ability to get information for
     * @return Key value pairs of information about the ability
     * @throws DynException Thrown on fetch error
     */
    public Map<String,Object> getAbility(String abilityName) throws DynException;
    
    /**
     * Creates the ability indentified by the given name if possible.
     * 
     * @param abilityName Name of the ability
     * @return true if the ability was created, false if nothing was todo
     * @throws DynException Thrown on fetch error
     */
    public boolean createAbilityIfNotExists(String abilityName) throws DynException;
    
    /**
     * Creates all abilities that do not exists and given in the collection
     * 
     * @param abilityNames Names of abilities
     * @return true if at least one ability was created, false if nothing was todo
     * @throws DynException Thrown on ability creation error
     */
    public boolean createAbilitiesIfNotExists(Collection<String> abilityNames) throws DynException;
    
    /**
     * Creates a storage if it is not existend
     *
     * @param name Name of the storage to create
     * @return true if storage was created, false if nothing was todo
     * @throws DynException Thrown on fetch error
     */
    public boolean createStorageIfNotExists(String name) throws DynException;
    
    /**
     * Checks if a storage exists (and is accessible over the connection)
     * 
     * @param name Name of the storage to search
     * @return true if the storage exists, false otherwise
     * 
     * @throws DynException 
     */
    public boolean storageExists(String name) throws DynException;
    
    /**
     * Creates all storages that do not exists and given in the collection
     * 
     * @param storageNames Collection of storage names
     * @return true if at least one storage was created, false if nothing was todo
     * @throws DynException Thrown on creation error
     */
    public boolean createStorageIfNotExists(Collection<String> storageNames) throws DynException;
    
    /**
     * Gets information about the storage with the given name
     * 
     * @param name Name of the storage where informations are requested
     * @return Key-value pairs with information about the storage
     * @throws DynException Thrown on fetch error
     */
    public Map<String,Object> getStorage(String name) throws DynException;
    
    /**
     * Returns a list of available collections in the given storage
     * 
     * @param name Name of the storage
     * @return List of collection names
     * @throws DynException 
     */
    public List<DataCollection> getCollections(String name) throws DynException;
    
    /**
     * Deletes the given storage and all its contents
     * 
     * @param name Name of the storage
     * @return true if storage was existend and is deleted
     * @throws DynException 
     */
    public boolean deleteStorage(String name) throws DynException;
}
