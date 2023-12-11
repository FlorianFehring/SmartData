package de.fhbielefeld.smartdata.dynrecords.filter;

import de.fhbielefeld.smartdata.converter.DataConverter;
import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.smartdata.dbo.Attribute;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import de.fhbielefeld.smartdata.dyncollection.DynCollection;

/**
 * Filter class for radius filters
 *
 * @author Lukas Stoll
 */
public class RadiusFilter extends Filter {

     
    private Object longitude;
    private Object latitude;
    private Object srid;
    private Object radius;
    

    public RadiusFilter(DynCollection table) {
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
            
            for (int i = 2; i <= 5; i++){
                if (checkIfNumeric(parts[i]) == false){
                    throw new FilterException("The Parameter at position " +(i+1) +" is not a Number!");
                }     
            }

            if (col == null) {
                throw new FilterException("The Column >" + this.attribute + "< does not exists.");
            }
            switch (col.getType()) {
                case "geometry":
                    this.longitude = DataConverter.objectToDouble(parts[2]);
                    this.latitude = DataConverter.objectToDouble(parts[3]);
                    this.srid = DataConverter.objectToInteger(parts[4]);
                    this.radius = DataConverter.objectToDouble(parts[5]);
                    break;
                default:
                    Message msg = new Message(
                            "RadiusFilter", MessageLevel.ERROR,
                            "Column type >" + col.getType() + "< is not supported. Please choose a Column with type geometry.");
                    Logger.addDebugMessage(msg);
            }

        } catch (DynException ex) {
            FilterException fex = new FilterException("Could not parse RadiusFilter: " + ex.getLocalizedMessage());
            fex.addSuppressed(ex);
            throw fex;
        }
    }

    @Override
    public String getPrepareCode() {
        return "ST_DWithin(geography(ST_Transform("+ this.attribute +",4326)),geography(ST_Transform(ST_SetSRID(ST_MakePoint(?, ?), ?),4326)), ?)";
    }

    @Override
    public PreparedStatement setFilterValue(PreparedStatement pstmt) throws FilterException {
        int pos = this.firstPlaceholder;
        try {
            pstmt.setDouble(pos, (Double) this.longitude);
            pstmt.setDouble(pos+1 , (Double) this.latitude);  
            pstmt.setInt(pos+2, (Integer) this.srid);
            pstmt.setDouble(pos+3, (Double) this.radius);
                  
        } catch (SQLException ex) {
            FilterException fex = new FilterException("Could not set value");
            fex.addSuppressed(ex);
            throw fex;
        }
        
        return pstmt;            
    }
    
    private boolean checkIfNumeric (String string){
        return string.matches("-?\\d+(\\.\\d+)?");
    }
}
