package de.fhbielefeld.smartdata.dbo;

import jakarta.xml.bind.annotation.*;

/**
 * Represents attribute information
 * 
 * @author Florian Fehring
 */
@XmlRootElement
public class Attribute {
    
    private String name;
    private String type;
    private String subtype;
    private Integer srid;
    private Integer dimension;
    private boolean isNullable = true;
    private boolean isIdentity = false;
    private String defaultvalue = null;
    private boolean isAutoIncrement = false;
    private String refName = null;
    private String refCollection = null;
    private String refStorage = null;
    private String refAttribute = null;
    private String refOnUpdate = null;
    private String refOnDelete = null;
    
    public Attribute() {
        
    }
    
    public Attribute(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSubtype() {
        return subtype;
    }

    public void setSubtype(String subtype) {
        this.subtype = subtype;
    }

    public Integer getSrid() {
        return srid;
    }

    public void setSrid(Integer srid) {
        this.srid = srid;
    }

    public Integer getDimension() {
        return dimension;
    }

    public void setDimension(Integer dimension) {
        this.dimension = dimension;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public void setIsNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }

    public boolean isIdentity() {
        return isIdentity;
    }

    public void setIsIdentity(boolean isIdentity) {
        this.isIdentity = isIdentity;
    }

    public String getDefaultvalue() {
        return defaultvalue;
    }

    public void setDefaultvalue(String defaultvalue) {
        this.defaultvalue = defaultvalue;
    }

    public boolean isIsAutoIncrement() {
        return isAutoIncrement;
    }

    public void setIsAutoIncrement(boolean isAutoIncrement) {
        this.isAutoIncrement = isAutoIncrement;
    }

    public String getRefName() {
        return refName;
    }

    public void setRefName(String refName) {
        this.refName = refName;
    }

    public String getRefCollection() {
        return refCollection;
    }

    public void setRefCollection(String refCollection) {
        this.refCollection = refCollection;
    }

    public String getRefStorage() {
        return refStorage;
    }

    public void setRefStorage(String refStorage) {
        this.refStorage = refStorage;
    }

    public String getRefAttribute() {
        return refAttribute;
    }

    public void setRefAttribute(String refAttribute) {
        this.refAttribute = refAttribute;
    }

    public String getRefOnUpdate() {
        return refOnUpdate;
    }

    public void setRefOnUpdate(String refOnUpdate) {
        this.refOnUpdate = refOnUpdate;
    }

    public String getRefOnDelete() {
        return refOnDelete;
    }

    public void setRefOnDelete(String refOnDelete) {
        this.refOnDelete = refOnDelete;
    }
}
