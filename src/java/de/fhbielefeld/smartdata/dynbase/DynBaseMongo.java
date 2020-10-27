package de.fhbielefeld.smartdata.dynbase;

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
public class DynBaseMongo extends DynMongo implements DynBase {

    public DynBaseMongo() throws DynException {
        this.connect();
    }
    
    @Override
    public Collection<String> getAbilities() throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Map<String, Object> getAbility(String abilityName) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean createAbilityIfNotExists(String abilityName) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
