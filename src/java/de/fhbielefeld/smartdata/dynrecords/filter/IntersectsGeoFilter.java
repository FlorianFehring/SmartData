package de.fhbielefeld.smartdata.dynrecords.filter;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.smartdata.dbo.Attribute;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import de.fhbielefeld.smartdata.dyncollection.DynCollection;

/**
 * Filter class for intersects geofilter
 *
 * @author Alexej Rogalski
 */
public class IntersectsGeoFilter extends Filter {
     
    private String geometry = "";
    

    public IntersectsGeoFilter(DynCollection table) {
        super(table);
    }
    
    @Override
    public void parse(String filtercode) throws FilterException {
        this.filtercode = filtercode;
        try {
            String[] parts = filtercode.split(",");
            // First element is the name of the attribute wanted to filter
            this.attribute = parts[0];
            // Check if the collection contains such a attribute
            Attribute col = this.collection.getAttribute(this.attribute);

            if (col == null) {
                throw new FilterException("The Column >" + this.attribute + "< does not exists.");
            }
            switch (col.getType()) {
                case "geometry":
                    for (int i = 2; i < parts.length; i++) {
                        this.geometry = this.geometry.concat(parts[i]);
                        if (i < parts.length - 1) {
                            this.geometry = this.geometry.concat(", ");
                        }
                    }
                    break;
                default:
                    Message msg = new Message(
                            "IntersectsGeoFilter", MessageLevel.ERROR,
                            "Column type >" + col.getType() + "< is not supported. Please choose a Column with type geometry.");
                    Logger.addDebugMessage(msg);
            }

        } catch (DynException ex) {
            FilterException fex = new FilterException("Could not parse IntersectsGeoFilter: " + ex.getLocalizedMessage());
            fex.addSuppressed(ex);
            throw fex;
        }
    }

    @Override
    public String getPrepareCode() {
        return "ST_Intersects(" + this.attribute + ", ST_GeomFromText(?))";
    }

    @Override
    public PreparedStatement setFilterValue(PreparedStatement pstmt) throws FilterException {
        int pos = this.firstPlaceholder;
        try {
            pstmt.setString(pos, (String) this.geometry);
                  
        } catch (SQLException ex) {
            FilterException fex = new FilterException("Could not set value");
            fex.addSuppressed(ex);
            throw fex;
        }
        
        return pstmt;            
    }
    
}