package de.fhbielefeld.smartdata.dyn;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import de.fhbielefeld.smartdata.config.Configuration;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.util.ArrayList;
import java.util.List;

/**
 * Basic database accessing methods
 * @author Florian Fehring
 */
public class DynMongo implements Dyn {

    protected MongoClient client;
    protected List<String> warnings = new ArrayList<>();
    
    @Override
    public void connect() throws DynException {
        Configuration conf = new Configuration();
        String mongourl = conf.getProperty("mongo.url");        
        this.client = MongoClients.create(mongourl);
    }

    @Override
    public void close() throws DynException {
        // No closeing needed here
    }
    
    @Override
    public List<String> getWarnings() {
        return warnings;
    }
}
