package de.fhbielefeld.smartdata.dbo;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Represents tables
 * 
 * @author Florian Fehring
 */
@XmlRootElement
public class Table {
    
    private String name;
    @XmlElementWrapper(name = "columns")
    @XmlElement(name = "columns")
    private ArrayList<Column> columns;
    
    public Table() {
        
    }
    
    public Table(String name) {
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
