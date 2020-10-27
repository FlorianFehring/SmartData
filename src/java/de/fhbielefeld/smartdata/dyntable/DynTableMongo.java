package de.fhbielefeld.smartdata.dyntable;

import de.fhbielefeld.smartdata.dbo.Column;
import de.fhbielefeld.smartdata.dbo.Table;
import de.fhbielefeld.smartdata.dyn.DynMongo;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.util.List;
import java.util.Map;

/**
 * Tabel operations for mongodb
 *
 * @author Florian Fehring
 */
public class DynTableMongo extends DynMongo implements DynTable {

    private String src;
    private String name;

    public DynTableMongo(String src, String name) throws DynException {
        this.connect();
        this.src = src;
        this.name = name;
    }

    @Override
    public boolean exists() throws DynException {
        for (String tbname : this.client.getDatabase(this.src).listCollectionNames()) {
            if (tbname.equals(this.name)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean create(Table table) throws DynException {
        if(this.exists()) {
            return false;
        }
        this.client.getDatabase(this.src).createCollection(table.getName());
        return true;
    }

    @Override
    public boolean addColumns(List<Column> columns) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Map<String, Column> getColumns() throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Column getColumn(String name) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<Column> getIdentityColumns() throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void changeColumnName(String oldname, String newname) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void changeColumns(List<Column> columns) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void delete() throws DynException {
        this.client.getDatabase(this.src).getCollection(this.name).drop();
    }
}
