package de.fhbielefeld.smartdata.dyndata;

import de.fhbielefeld.smartdata.dyndata.filter.Filter;
import de.fhbielefeld.smartdata.dyntable.DynTable;
import de.fhbielefeld.smartdata.dyntable.DynTablePostgres;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.json.JsonObject;

/**
 * Class for manageing data
 *
 * @author Florian Fehring
 */
public abstract class DynData {

    protected String schema;
    protected String table;
    protected Connection con;

    protected List<String> warnings = new ArrayList<>();
    protected DynTable dyntable = null;
    protected static final Map<String, PreparedStatement> preparedStatements = new HashMap<>();
    protected static final Map<String, Map<String, Integer>> preparedPlaceholders = new HashMap<>();

    public DynData(String schema, String table, Connection con) {
        this.schema = schema;
        this.table = table;
        this.con = con;

        // Get available columns
        this.dyntable = new DynTablePostgres(this.schema, this.table, this.con);
    }
    
    /**
     * Get the warnings occured while processing
     * 
     * @return List of warning messages
     */
    public List<String> getWarnings() {
        return warnings;
    }

    /**
     * Builds up the sql statement requied for selecting the data.All prameters
     * are optional.
     *
     * @param includes Commata separated list of names of columns to include
     * @param filters Filter objects applicable
     * @param size Maximum number of datasets
     * @param page Page to fetch from database (pageno[,pagesize])
     * @param order Order to order after (column[,[ASC|DESC]])
     * @param countOnly If true ounly deliver count of sets
     * @param unique Column tahts contents should be returned unique
     * @param deflatt If true a single dataset should be deflatted into multiple
     * datasets
     * @return Id of the generated statement and placeholdermap
     * @throws de.fhbielefeld.smartdata.exceptions.DynException
     */
    public abstract String getPreparedQuery(String includes, Collection<Filter> filters, int size, String page, String order, boolean countOnly, String unique, boolean deflatt) throws DynException;

    /**
     * Sets the clauses (WHERE, LIMIT, ...) on a prepared statement.
     *
     * @param stmtid Id of the prepared statement to use
     * @param filters Filter objects applicable
     * @param size Maximum number of datasets
     * @param page Page to fetch from database (pageno[,pagesize])
     *
     * @return NativeQuery with setted clauses
     * @throws de.fhbielefeld.smartdata.exceptions.DynException
     */
    protected abstract PreparedStatement setQueryClauses(String stmtid, Collection<Filter> filters, int size, String page) throws DynException;

    /**
     * Gets data from the database
     *
     * @param includes Name of the columns that should be included, if not given
     * all available columns are delivered
     * @param filters Filters to apply
     * @param size Maximum number of datasets
     * @param page Page to fetch from database (pageno[,pagesize])
     * @param order Order to order after (column[,[ASC|DESC]])
     * @param countOnly If true ounly deliver count of sets
     * @param unique Column tahts contents should be returned unique
     * @param deflatt If true a single dataset should be deflatted into multiple
     * datasets
     *
     * @return JSON representation of the data
     * @throws DynException
     */
    public abstract String get(String includes, Collection<Filter> filters, int size, String page, String order, boolean countOnly, String unique, boolean deflatt) throws DynException;

    /**
     * Prepares the code for insertion of data
     *
     * @param json Json object containing the data
     *
     * @return Id of the prepared statement
     * @throws DynException
     */
    public abstract String getPreparedInsert(JsonObject json) throws DynException;

    /**
     * Creates a dataset from the given json and inserts it into table
     *
     * @param json JSON String with one-level hierarchy
     *
     * @return Id of the new created dataset
     * @throws DynException
     */
    public abstract Long create(String json) throws DynException;

    /**
     * Creates a dataset from the given json and inserts it into table
     *
     * @param json JSON String with one-level hierarchy
     *
     * @return Id of the new created dataset
     * @throws DynException
     */
    public abstract Long create(JsonObject json) throws DynException;
    
    /**
     * GEt a prepared query for updateing datasets.
     * 
     * @param json Json object with data set up to date
     * @param id Id of the dataset to update (can be null if id is stored in json)
     * @return Id of the prepared statement
     * @throws DynException 
     */
    public abstract String getPreparedUpdate(JsonObject json, Long id) throws DynException;
    
    /**
     * Updates datasets with data given as a json string.
     * 
     * @param json Json string containing new values and indentity column values
     * @param id Id of the dataset to update (can be null if id is stored in json)
     * @return Number of updated datasets
     * @throws DynException 
     */
    public abstract Long update(String json, Long id) throws DynException;
    
    /**
     * Updates one dataset with the data given as json object
     * 
     * @param json Json Object containing data to update and identity column with value
     * @param id Id of the dataset to update (can be null if id is stored in json)
     * @return Number of updated datasets
     * @throws DynException 
     */
    public abstract Long update(JsonObject json, Long id) throws DynException;
}
