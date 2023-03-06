package de.fhbielefeld.smartdata.dynrecords;

import de.fhbielefeld.smartdata.dyn.DynMongo;
import de.fhbielefeld.smartdata.dynrecords.filter.Filter;
import de.fhbielefeld.smartdata.exceptions.DynException;
import jakarta.json.JsonObject;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.List;

/**
 * Methods for accessing data from a mongodb..
 * 
 * @author Florian Fehring
 */
public class DynRecordsMongo extends DynMongo implements DynRecords {

    @Override
    public String getPreparedQuery(String includes, Collection<Filter> filters, int size, String page, String order, boolean countOnly, String unique, boolean deflatt, String geojsonattr, String geotransform, Collection<String> joins) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public PreparedStatement setQueryClauses(String stmtid, Collection<Filter> filters, int size, String page) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String get(String includes, Collection<Filter> filters, int size, String page, String order, boolean countOnly, String unique, boolean deflatt, String geojsonattr, String geotransform, Collection<String> joins) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getPreparedInsert(JsonObject json) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<Object> create(String json) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object create(JsonObject json) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getPreparedUpdate(JsonObject json, Long id) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Long update(String json, Long id) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Long update(JsonObject json, Long id) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Long delete(String id) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
