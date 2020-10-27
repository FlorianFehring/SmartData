package de.fhbielefeld.smartdata.dbo;

import java.util.ArrayList;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Represents collections
 * 
 * @author Florian Fehring
 */
@XmlRootElement
public class DataCollection {
    
    private String name;
    @XmlElementWrapper(name = "attributes")
    @XmlElement(name = "attributes")
    private ArrayList<Attribute> attributes;
    
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
}
