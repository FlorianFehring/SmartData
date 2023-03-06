package de.fhbielefeld.smartdata.dbo;

import jakarta.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;

/**
 * Represents collections
 * 
 * @author Florian Fehring
 */
@XmlRootElement
//@XmlAccessorType(XmlAccessType.FIELD) // Should avoid dulicate attribute error, but causes content before problem
public class DataCollection {
    
    private String name;
//    @XmlElementWrapper(name = "attributes")
//    @XmlElement(name = "attributes")
    private ArrayList<Attribute> attributes = new ArrayList<>();
    
    public DataCollection() {
        
    }
    
    public DataCollection(String name) {
        this.name = name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getName() {
        return this.name;
    }

    public ArrayList<Attribute> getAttributes() {
        return attributes;
    }

    public void setAttributes(ArrayList<Attribute> attributes) {
        this.attributes = attributes;
    }
    
    public void addAttribute(Attribute attribute) {
        this.attributes.add(attribute);
    }
    
    /**
     * Returns the identity column of this collection
     * 
     * @return identity collumn or null if not exists
     */
    public Attribute getIdentityColum() {
        for(Attribute curAttr : this.attributes) {
            if(curAttr.isIdentity()) {
                return curAttr;
            }
        }
        return null;
    }
}
