package de.fhbielefeld.smartdata.dyn;

import de.fhbielefeld.smartdata.config.Configuration;
import de.fhbielefeld.smartdata.dyncollection.DynCollection;
import de.fhbielefeld.smartdata.dyncollection.DynCollectionMongo;
import de.fhbielefeld.smartdata.dyncollection.DynCollectionPostgres;
import de.fhbielefeld.smartdata.dynrecords.DynRecords;
import de.fhbielefeld.smartdata.dynrecords.DynRecordsMongo;
import de.fhbielefeld.smartdata.dynrecords.DynRecordsPostgres;
import de.fhbielefeld.smartdata.dynstorage.DynStorage;
import de.fhbielefeld.smartdata.dynstorage.DynStorageMongo;
import de.fhbielefeld.smartdata.dynstorage.DynStoragePostgres;
import de.fhbielefeld.smartdata.exceptions.DynException;

/**
 * Factory for createing Dyn instances
 *
 * @author Florian
 */
public class DynFactory {

    /**
     * Get a DynCollection for storage access
     *
     * @param storage Storages name
     * @param collection Collections name
     * @return DynCollection as configured in configuration
     * @throws DynException
     */
    public static DynCollection getDynCollection(String storage, String collection) throws DynException {
        Configuration conf = new Configuration();
        if (conf.getProperty("mongo.url") != null) {
            return new DynCollectionMongo(storage, collection);
        } else {
            return new DynCollectionPostgres(storage, collection);
        }
    }

    /**
     * Get a DynRecords for storage access
     *
     * @param storage Storages name
     * @param collection Collections name
     * @return DynRecords as configured in configuration
     * @throws DynException
     */
    public static DynRecords getDynRecords(String storage, String collection) throws DynException {
        Configuration conf = new Configuration();
        if (conf.getProperty("mongo.url") != null) {
            return new DynRecordsMongo();
        } else {
            return new DynRecordsPostgres(storage, collection);
        }
    }

    /**
     * Get a DynStorage for storage access
     *
     * @return DynStorage as configured in configuration
     * @throws DynException
     */
    public static DynStorage getDynStorage() throws DynException {
        Configuration conf = new Configuration();
        if (conf.getProperty("mongo.url") != null) {
            return new DynStorageMongo();
        } else {
            return new DynStoragePostgres();
        }
    }
    
//    static void cache() {
//            DynCollection dync;
//        DynRecords dynr;
//
//        if (dynColCache.containsKey(storage + "_" + collection)) {
//            dync = dynColCache.get(storage + "_" + collection);
//            dynr = dynRecCache.get(storage + "_" + collection);
//        } else {
//            Configuration conf = new Configuration();
//                if (conf.getProperty("mongo.url") != null) {
//                    dync = new DynCollectionMongo(storage, collection);
//                    dynr = new DynRecordsMongo();
//                } else {
//                    dync = new DynCollectionPostgres(storage, collection);
//                    dynr = new DynRecordsPostgres(storage, collection);
//                }
//                dynColCache.put(storage + "_" + collection, dync);
//                dynRecCache.put(storage + "_" + collection, dynr);
//        }
//    }
}
