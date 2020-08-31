package de.fhbielefeld.smartdata.dyndata.filter;

import de.fhbielefeld.smartdata.dyntable.DynTable;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents one filter for a search
 *
 * @author Florian Fehring
 */
public abstract class Filter {

    protected String filtercode;
    protected DynTable table;
    protected String column;
    protected int firstPlaceholder;
    // Noteing warnings for calling methods
    protected List<String> warnings = new ArrayList<>();

    public Filter(DynTable table) {
        this.table = table;
    }

    /**
     * Gets the original filter code
     * 
     * @return Filtercode
     */
    public String getFiltercode() {
        return filtercode;
    }
    
    /**
     * Parses the filtercode and creates the filter rule.
     *
     * @param filtercode Filter expression (e.g. column,eq,2)
     * @throws FilterException
     */
    public abstract void parse(String filtercode) throws FilterException;

    /**
     * Creates sql for addint to a query useable for prepared statemnts
     *
     * @return sql code for use in prepared statements
     */
    public abstract String getPrepareCode();

    /**
     * Get the count of placeholdes in this filter
     * 
     * @return Number of placeholders
     */
    public int getNumberOfPlaceholders() {
        String prepcode = this.getPrepareCode();
        int count = 0;
        for (int i = 0; i < prepcode.length(); i++) {
            if (prepcode.charAt(i) == '?') {
                count++;
            }
        }
        return count;
    }

    public int getFirstPlaceholder() {
        return firstPlaceholder;
    }

    public void setFirstPlaceholder(int firstPlaceholder) {
        this.firstPlaceholder = firstPlaceholder;
    }
    
    /**
     * Sets the value of this filter on its position
     *
     * @param pstmt Prepared statemnt to set filter value to
     * @return Modified PreparedStatement
     * @throws de.fhbielefeld.smartdata.dyndata.filter.FilterException
     */
    public abstract PreparedStatement setFilterValue(PreparedStatement pstmt) throws FilterException;

    /**
     * Get the warnings given by the filter while setting filter value.
     * 
     * @return List of warning messages
     */
    public List<String> getWarnings() {
        return warnings;
    }
}
