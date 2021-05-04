package de.fhbielefeld.smartdata.dynrecords.filter;

import de.fhbielefeld.smartdata.converter.DataConverter;
import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.smartdata.dbo.Attribute;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.Month;
import de.fhbielefeld.smartdata.dyncollection.DynCollection;

/**
 * Filter class for between filters
 *
 * @author Alexej Rogalski
 */
public class BetweenFilter extends Filter {

    private Object btvalueFrom;
	private Object btvalueTo;

    public BetweenFilter(DynCollection table) {
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
            // Thrid und fourth elements are the limit values
            switch (col.getType()) {
                case "real":
                case "double":
		case "float4":
		case "float8":
                    this.btvalueFrom = DataConverter.objectToDouble(parts[2]);
                    this.btvalueTo = DataConverter.objectToDouble(parts[3]);
                    break;
                case "int2":
                    this.btvalueFrom = DataConverter.objectToShort(parts[2]);
                    this.btvalueTo = DataConverter.objectToShort(parts[3]);
                    break;
                case "int4":
                case "int8":
                    this.btvalueFrom = DataConverter.objectToInteger(parts[2]);
                    this.btvalueTo = DataConverter.objectToInteger(parts[3]);
                    break;
                case "timestamp with timezone":
                case "timestamp":
                    this.btvalueFrom = DataConverter.objectToLocalDateTime(parts[2]);
                    this.btvalueTo = DataConverter.objectToLocalDateTime(parts[3]);
                    break;
                default:
                    Message msg = new Message(
                            "BetweenFilter", MessageLevel.WARNING,
                            "Column type >" + col.getType() + "< is currently not supported.");
                    Logger.addDebugMessage(msg);
            }

        } catch (DynException ex) {
            FilterException fex = new FilterException("Could not parse BetweenFilter: " + ex.getLocalizedMessage());
            fex.addSuppressed(ex);
            throw fex;
        }
    }

    @Override
    public String getPrepareCode() {
        if (this.negative) {
            return this.attribute + " NOT BETWEEN ? AND ?";
        } else {
            return this.attribute + " BETWEEN ? AND ?";
        }
    }

    @Override
    public PreparedStatement setFilterValue(PreparedStatement pstmt) throws FilterException {
        int pos = this.firstPlaceholder;
        try {
            if (this.btvalueFrom.getClass().equals(Integer.class)) {
                pstmt.setInt(pos, (Integer) this.btvalueFrom);
                pstmt.setInt(pos + 1, (Integer) this.btvalueTo);
            } else if (this.btvalueFrom.getClass().equals(Double.class)) {
                pstmt.setDouble(pos, (Double) this.btvalueFrom);
		pstmt.setDouble(pos + 1, (Double) this.btvalueTo);
            } else if (this.btvalueFrom.getClass().equals(LocalDateTime.class)) {
                LocalDateTime ldt = (LocalDateTime) this.btvalueFrom;
                LocalDateTime ldt2 = (LocalDateTime) this.btvalueTo;
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
                if (ldt2.isBefore(minDateTime)) {
                    ldt2 = minDateTime;
                    this.warnings.add("The DB only supports Julian Time. StartDate will is set to 01-01-4713BC");
                } else if (ldt2.isAfter(maxDateTime)) {
                    ldt2 = maxDateTime;
                    this.warnings.add("The DB only supports Julian Time. StartDate will is set to 31-12-294276AD");
                }
                if (ldt2.isBefore(ewDateTime)) {
                    ldt2 = ldt2.plusYears(1);
                }
                Timestamp ts = Timestamp.valueOf(ldt);
                Timestamp ts2 = Timestamp.valueOf(ldt2);
                pstmt.setTimestamp(pos, ts);
                pstmt.setTimestamp(pos + 1, ts2);
            }
        } catch (SQLException ex) {
            FilterException fex = new FilterException("Could not set value");
            fex.addSuppressed(ex);
            throw fex;
        }

        return pstmt;
    }
}