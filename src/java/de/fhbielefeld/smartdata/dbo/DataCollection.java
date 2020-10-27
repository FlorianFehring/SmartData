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
    @XmlElementWrapper(name = "columns")
    @XmlElement(name = "columns")
    private ArrayList<Column> columns;
    
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

    public ArrayList<Column> getColumns() {
        return columns;
    }

    public void setColumns(ArrayList<Column> columns) {
        this.columns = columns;
    }
    
    public void addColumn(Column column) {
        this.columns.add(column);
    }
}
