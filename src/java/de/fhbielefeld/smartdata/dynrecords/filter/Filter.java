package de.fhbielefeld.smartdata.dynrecords.filter;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import de.fhbielefeld.smartdata.dyncollection.DynCollection;

/**
 * Represents one filter for a search
 *
 * @author Florian Fehring
 */
public abstract class Filter {

    protected String filtercode;
    protected DynCollection collection;
    protected String attribute;
    protected int firstPlaceholder;
    protected boolean negative;
    // Noteing warnings for calling methods
    protected List<String> warnings = new ArrayList<>();

    public Filter(DynCollection collection) {
        this.collection = collection;
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
     * @param filtercode Filter expression (e.g. attribute,eq,2)
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
     * @throws de.fhbielefeld.smartdata.dynrecords.filter.FilterException
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
