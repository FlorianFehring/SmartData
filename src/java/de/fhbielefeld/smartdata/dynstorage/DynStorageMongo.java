package de.fhbielefeld.smartdata.dynstorage;

import com.mongodb.client.MongoDatabase;

import de.fhbielefeld.smartdata.dbo.DataCollection;
import de.fhbielefeld.smartdata.dyn.DynMongo;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manage functionalities for mongodb
 * @author Florian Fehring
 */
public class DynStorageMongo extends DynMongo implements DynStorage {

    public DynStorageMongo() throws DynException {
        this.connect();
    }
    
    @Override
    public Map<String,String> getAbilities() throws DynException {
        Map<String, String> abilities = new HashMap<>();
        abilities.put("gis", "MongoDB geospatial features");
        return abilities;
    }

    @Override
    public boolean createAbilityIfNotExists(String abilityName) throws DynException {
        boolean created = false;
        if(abilityName.equalsIgnoreCase("gis")) {
            // geospatial features are integrated in Mongo no need to install
        }
        return created;
    }

    @Override
    public boolean createStorageIfNotExists(Collection<String> storageNames) throws DynException {
        boolean created = false;
        for (String storageName : storageNames) {
            if (this.createStorageIfNotExists(storageName)) {
                created = true;
            }
        }
        return created;
    }

    @Override
    public boolean createStorageIfNotExists(String name) throws DynException {
        if (!this.storageExists(name)) {
            this.client.getDatabase(name);
            return true;
        }
        return false;
    }

    @Override
    public boolean createAbilitiesIfNotExists(Collection<String> abilityNames) throws DynException {
        boolean created = false;
        for (String abilityName : abilityNames) {
            if (this.createAbilityIfNotExists(abilityName)) {
                created = true;
            }
        }
        return created;
    }

    @Override
    public boolean storageExists(String name) throws DynException {
        MongoDatabase md = this.client.getDatabase(name);
        if (md == null) {
            return false;
        }
        return true;
    }

    @Override
    public Map<String, Object> getStorage(String name) throws DynException {
        Map<String, Object> information = new HashMap<>();
        information.put("name", name);
        return information;
    }

    @Override
    public List<DataCollection> getCollections(String name) throws DynException {
        List<DataCollection> collections = new ArrayList<>();
        MongoDatabase mdb = this.client.getDatabase(name);
        for (String tname : mdb.listCollectionNames()) {
            collections.add(new DataCollection(tname));
        }
        return collections;
    }

    @Override
    public boolean deleteStorage(String name) throws DynException {
        if (this.storageExists(name)) {
            this.client.getDatabase(name).drop();
            return true;
        }
        return false;
    }
}
