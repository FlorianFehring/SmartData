package de.fhbielefeld.smartdata.dyncollection;

import de.fhbielefeld.smartdata.dbo.Attribute;
import de.fhbielefeld.smartdata.dbo.DataCollection;
import de.fhbielefeld.smartdata.dyn.DynMongo;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tabel operations for mongodb
 *
 * @author Florian Fehring
 */
public class DynCollectionMongo extends DynMongo implements DynCollection {

    private String src;
    private String name;

    public DynCollectionMongo(String src, String name) throws DynException {
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
    public boolean create(DataCollection table) throws DynException {
        if(this.exists()) {
            return false;
        }
        this.client.getDatabase(this.src).createCollection(table.getName());
        return true;
    }

    @Override
    public boolean addAttributes(List<Attribute> attributes) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public boolean delAttributes(List<Attribute> attributes) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    public Map<String, Attribute> getAttributes() throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Attribute getAttribute(String name) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Attribute getReferenceTo(String collection) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public List<Attribute> getIdentityAttributes() throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<Attribute> getGeoAttributes() throws DynException {
        List<Attribute> geoattrs = new ArrayList<>();
        for (Attribute curAttr : this.getAttributes().values()) {
            if (curAttr.getType().equalsIgnoreCase("geometry")) {
                geoattrs.add(curAttr);
            }
        }
        return geoattrs;
    }
    
    @Override
    public void changeAttributeName(String oldname, String newname) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void changeAttributes(List<Attribute> attributes) throws DynException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void delete() throws DynException {
        this.client.getDatabase(this.src).getCollection(this.name).drop();
    }
}
