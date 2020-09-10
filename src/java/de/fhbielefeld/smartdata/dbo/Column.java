package de.fhbielefeld.smartdata.dbo;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Represemts column information
 * 
 * @author Florian Fehring
 */
@XmlRootElement
public class Column {
    
    private String name;
    private String type;
    private boolean isNullable = true;
    private boolean isIdentity = false;
    
    public Column() {
        
    }
    
    public Column(String name, String type) {
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
}
