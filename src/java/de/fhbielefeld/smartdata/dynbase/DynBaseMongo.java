package de.fhbielefeld.smartdata.dynbase;

import com.mongodb.client.MongoDatabase;

import de.fhbielefeld.smartdata.dbo.Table;
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
    public boolean createSchemaIfNotExists(Collection<String> schemataNames) throws DynException {
        boolean created = false;
        for (String schemaName : schemataNames) {
            if (this.createSchemaIfNotExists(schemaName)) {
                created = true;
            }
        }
        return created;
    }

    @Override
    public boolean createSchemaIfNotExists(String schemaName) throws DynException {
        if (!this.schemaExists(schemaName)) {
            this.client.getDatabase(schemaName);
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
    public boolean schemaExists(String schemaName) throws DynException {
        MongoDatabase md = this.client.getDatabase(schemaName);
        if (md == null) {
            return false;
        }
        return true;
    }

    @Override
    public Map<String, Object> getSchema(String schemaName) throws DynException {
        Map<String, Object> information = new HashMap<>();
        information.put("name", schemaName);
        return information;
    }

    @Override
    public List<Table> getTables(String schemaName) throws DynException {
        List<Table> tables = new ArrayList<>();
        MongoDatabase mdb = this.client.getDatabase(schemaName);
        for (String tname : mdb.listCollectionNames()) {
            tables.add(new Table(tname));
        }
        return tables;
    }

    @Override
    public boolean deleteSchema(String schemaName) throws DynException {
        if (this.schemaExists(schemaName)) {
            this.client.getDatabase(schemaName).drop();
            return true;
        }
        return false;
    }
}
