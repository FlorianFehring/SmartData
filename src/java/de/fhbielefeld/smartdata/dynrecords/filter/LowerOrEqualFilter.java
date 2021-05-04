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
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.Month;

/**
 * Filter class for lower or equal filters
 *
 * @author Alexej Rogalski
 */
public class LowerOrEqualFilter extends Filter {

    private Object levalue;

    public LowerOrEqualFilter(DynCollection table) {
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
            // Second element is the name of the filter
            // Check if the filter is negative
            if (parts[1].startsWith("n")) {
                this.negative = true;
            }
            // Thrid element is the value that should be lower or equal
            switch (col.getType()) {
                case "real":
                case "double":
                case "float4":
                case "float8":
                    this.levalue = DataConverter.objectToDouble(parts[2]);
                    break;
                case "int2":
                    this.levalue = DataConverter.objectToShort(parts[2]);
                    break;
                case "int4":
                    this.levalue = DataConverter.objectToInteger(parts[2]);
                    break;
                case "int8":
                    this.levalue = DataConverter.objectToInteger(parts[2]);
                    break;
                case "timestamp with timezone":
                case "timestamp":
                    this.levalue = DataConverter.objectToLocalDateTime(parts[2]);
                    break;
                default:
                    Message msg = new Message(
                            "LowerOrEqualFilter", MessageLevel.WARNING,
                            "Column type >" + col.getType() + "< is currently not supported.");
                    Logger.addDebugMessage(msg);
            }

        } catch (DynException ex) {
            FilterException fex = new FilterException("Could not parse LowerOrEqualFilter: " + ex.getLocalizedMessage());
            fex.addSuppressed(ex);
            throw fex;
        }
    }

    @Override
    public String getPrepareCode() {
        if (this.negative) {
            return this.attribute + " > ?";
        } else {
            return this.attribute + " <= ?";
        }
    }

    @Override
    public PreparedStatement setFilterValue(PreparedStatement pstmt) throws FilterException {
        int pos = this.firstPlaceholder;
        try {
            if (this.levalue.getClass().equals(Integer.class)) {
                pstmt.setInt(pos, (Integer) this.levalue);
            } else if (this.levalue.getClass().equals(Double.class)) {
                pstmt.setDouble(pos, (Double) this.levalue);
            } else if (this.levalue.getClass().equals(LocalDateTime.class)) {
                LocalDateTime ldt = (LocalDateTime) this.levalue;
                // Check startdate and enddate on min and max supported values
                LocalDateTime minDateTime = LocalDateTime.of(-4713, Month.JANUARY, 1, 0, 0);
                LocalDateTime maxDateTime = LocalDateTime.of(294276, Month.DECEMBER, 31, 0, 0);
                LocalDateTime ewDateTime = LocalDateTime.of(0, Month.JANUARY, 1, 0, 0);

                // Check if starttime is not to far BC or in future
                if (ldt.isBefore(minDateTime)) {
                    ldt = minDateTime;
                    this.warnings.add("The DB only supports Julian Time. StartDate will is set to 01-01-4713BC");
                } else if (ldt.isAfter(maxDateTime)) {
                    ldt = maxDateTime;
                    this.warnings.add("The DB only supports Julian Time. StartDate will is set to 31-12-294276AD");
                }
                if (ldt.isBefore(ewDateTime)) {
                    ldt = ldt.plusYears(1);
                }

                Timestamp ts = Timestamp.valueOf(ldt);
                pstmt.setTimestamp(pos, ts);
            }
        } catch (SQLException ex) {
            FilterException fex = new FilterException("Could not set value");
            fex.addSuppressed(ex);
            throw fex;
        }

        return pstmt;
    }
}