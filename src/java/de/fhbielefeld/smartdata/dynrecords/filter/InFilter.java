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
 * Filter class for in filters
 *
 * @author Alexej Rogalski
 */
public class InFilter extends Filter {

    private Object[] invalues;
    private int numberOfPlaceholders;

    public InFilter(DynCollection table) {
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
            this.numberOfPlaceholders = parts.length - 2;
            // Further elements are in comma separated list of values
            switch (col.getType()) {
                case "text":
                case "varchar":
                    this.invalues = new String[this.numberOfPlaceholders];
                    for (int i = 0; i < this.numberOfPlaceholders; i++) {
			this.invalues[i] = parts[2 + i];
                    }
                    break;
                case "real":
                case "double":
                case "float4":
		case "float8":
                    this.invalues = new Double[this.numberOfPlaceholders];
                    for (int i = 0; i < this.numberOfPlaceholders; i++) {
                        this.invalues[i] = DataConverter.objectToDouble(parts[2 + i]);
                    }
                    break;
                case "int2":
                case "int4":
                case "int8":
                    this.invalues = new Integer[this.numberOfPlaceholders];
                    for (int i = 0; i < this.numberOfPlaceholders; i++) {
			this.invalues[i] = DataConverter.objectToInteger(parts[2 + i]);
                    }
                    break;
                case "timestamp with timezone":
                case "timestamp":
                    this.invalues = new LocalDateTime[this.numberOfPlaceholders];
                    for (int i = 0; i < this.numberOfPlaceholders; i++) {
			this.invalues[i] = DataConverter.objectToLocalDateTime(parts[2 + i]);
                    }
                    break;
                default:
                    Message msg = new Message(
                            "InFilter", MessageLevel.WARNING,
                            "Column type >" + col.getType() + "< is currently not supported.");
                    Logger.addDebugMessage(msg);
            }

        } catch (DynException ex) {
            FilterException fex = new FilterException("Could not parse InFilter: " + ex.getLocalizedMessage());
            fex.addSuppressed(ex);
            throw fex;
        }
    }

    @Override
    public String getPrepareCode() {
	String placeholders = "";
	for (int i = 0; i < this.numberOfPlaceholders; i++) {
            if (i == this.numberOfPlaceholders - 1) {
		placeholders = placeholders.concat("?");
            } else {
		placeholders = placeholders.concat("?, ");
            }
        }
        if (this.negative) {
            return this.attribute + " NOT IN (" + placeholders + ")";
        } else {
            return this.attribute + " IN (" + placeholders + ")";
        }
    }

    @Override
    public PreparedStatement setFilterValue(PreparedStatement pstmt) throws FilterException {
        int pos = this.firstPlaceholder;
        try {
            if (this.invalues.getClass().getComponentType().equals(String.class)) {
		for (int i = 0; i < this.numberOfPlaceholders; i++) {
                    pstmt.setString(pos + i, (String) this.invalues[i]);
		}
            } else if (this.invalues.getClass().getComponentType().equals(Integer.class)) {
		for (int i = 0; i < this.numberOfPlaceholders; i++) {
                    pstmt.setInt(pos + i, (Integer) this.invalues[i]);
		}
            } else if (this.invalues.getClass().getComponentType().equals(Double.class)) {
		for (int i = 0; i < this.numberOfPlaceholders; i++) {
                    pstmt.setDouble(pos + i, (Double) this.invalues[i]);
		}
            } else if (this.invalues.getClass().getComponentType().equals(LocalDateTime.class)) {
		for (int i = 0; i < this.numberOfPlaceholders; i++) {
                    LocalDateTime ldt = (LocalDateTime) this.invalues[i];
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
                    pstmt.setTimestamp(pos + i, ts);
		}
            }
        } catch (SQLException ex) {
            FilterException fex = new FilterException("Could not set value");
            fex.addSuppressed(ex);
            throw fex;
        }

        return pstmt;
    }
}