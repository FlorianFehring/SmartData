package de.fhbielefeld.smartdata.dyndata.filter;

import de.fhbielefeld.smartdata.converter.DataConverter;
import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.smartdata.dbo.Column;
import de.fhbielefeld.smartdata.dyntable.DynTable;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Filter class for bounding box filters
 *
 * @author Lukas Stoll
 */
public class BoundingBoxFilter extends Filter {

     
    private Object xmin;
    private Object ymin;
    private Object xmax;
    private Object ymax;
    private Object srid;
    private Object table_srid;

    public BoundingBoxFilter(DynTable table) {
        super(table);
    }
    
    @Override
    public void parse(String filtercode) throws FilterException {
        this.filtercode = filtercode;
        try {
            String[] parts = filtercode.split(",");
            // First element is the name of the column wanted to filter
            this.column = parts[0];
            // Check if the table contains such a column
            Column col = this.table.getColumn(this.column);
            
            for (int i = 2; i <= 7; i++){
                if (checkIfNumeric(parts[i]) == false){
                    throw new FilterException("The Parameter at position " +(i+1) +" is not a Number!");
                }     
            }

            if (col == null) {
                throw new FilterException("The Column >" + this.column + "< does not exists.");
            }
            switch (col.getType()) {
                case "geometry":
                    this.xmin = DataConverter.objectToDouble(parts[2]);
                    this.ymin = DataConverter.objectToDouble(parts[3]);
                    this.xmax = DataConverter.objectToDouble(parts[4]);
                    this.ymax = DataConverter.objectToDouble(parts[5]);
                    this.srid = DataConverter.objectToInteger(parts[6]);
                    this.table_srid = DataConverter.objectToInteger(parts[7]);
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
        return this.column +"@ ST_Transform((ST_MakeEnvelope( ?, ?, ?, ?, ?)), ?)";
    }

    @Override
    public PreparedStatement setFilterValue(PreparedStatement pstmt) throws FilterException {
        int pos = this.firstPlaceholder;
        try {
            pstmt.setDouble(pos, (Double) this.xmin);
            pstmt.setDouble(pos+1 , (Double) this.ymin);  
            pstmt.setDouble(pos+2, (Double) this.xmax);
            pstmt.setDouble(pos+3 , (Double) this.ymax); 
            pstmt.setInt(pos+4, (Integer) this.srid);
            pstmt.setInt(pos+5, (Integer) this.table_srid);
                  
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