package de.fhbielefeld.smartdata.dynrecords;

import de.fhbielefeld.smartdata.dyn.Dyn;
import de.fhbielefeld.smartdata.dynrecords.filter.Filter;
import de.fhbielefeld.smartdata.exceptions.DynException;
import jakarta.json.JsonObject;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.List;

/**
 * Class for manageing data
 *
 * @author Florian Fehring
 */
public interface DynRecords extends Dyn {


    /**
     * Builds up the sql statement requied for selecting the data.All prameters
     * are optional.
     *
     * @param includes Commata separated list of names of attributes to include
     * @param filters Filter objects applicable
     * @param size Maximum number of datasets
     * @param page Page to fetch from database (pageno[,pagesize])
     * @param order Order to order after (attribute[,[ASC|DESC]])
     * @param countOnly If true ounly deliver count of sets
     * @param unique Attribute tahts contents should be returned unique
     * @param deflatt If true a single dataset should be deflatted into multiple
     * datasets
     * @param geojsonattr Attribut that holds position for geojson
     * @param geotransform Type of the transform that should be applied to geometry columns (EPSG code or 'latlon')
     * @param joins Join expressions
     * 
     * @return Id of the generated statement and placeholdermap
     * @throws de.fhbielefeld.smartdata.exceptions.DynException
     */
    public abstract String getPreparedQuery(String includes, Collection<Filter> filters, int size, String page, String order, boolean countOnly, String unique, boolean deflatt, String geojsonattr, String geotransform, Collection<String> joins) throws DynException;

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
    public abstract PreparedStatement setQueryClauses(String stmtid, Collection<Filter> filters, int size, String page) throws DynException;

    /**
     * Gets data from the database
     *
     * @param includes Name of the attributes that should be included, if not given
     * all available attributes are delivered
     * @param filters Filters to apply
     * @param size Maximum number of datasets
     * @param page Page to fetch from database (pageno[,pagesize])
     * @param order Order to order after (attribute[,[ASC|DESC]])
     * @param countOnly If true ounly deliver count of sets
     * @param unique Attribute tahts contents should be returned unique
     * @param deflatt If true a single dataset should be deflatted into multiple
     * datasets
     * @param geojsonattr Attribut that holds data for geo location
     * @param geotransform Type of the transform that should be applied to geometry columns (EPSG code or 'latlon')
     * @param joins Join expressions
     *
     * @return JSON representation of the data
     * @throws DynException
     */
    public abstract String get(String includes, Collection<Filter> filters, int size, String page, String order, boolean countOnly, String unique, boolean deflatt, String geojsonattr, String geotransform, Collection<String> joins) throws DynException;

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
     * Creates a dataset from the given json and inserts it into collection
     *
     * @param json JSON String with one-level hierarchy
     *
     * @return Ids of the new created datasets
     * @throws DynException
     */
    public abstract List<Object> create(String json) throws DynException;

    /**
     * Creates a dataset from the given json and inserts it into collection
     *
     * @param json JSON String with one-level hierarchy
     *
     * @return Id of the new created datasets
     * @throws DynException
     */
    public abstract Object create(JsonObject json) throws DynException;
    
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
     * @param json Json string containing new values and indentity attribute values
     * @param id Id of the dataset to update (can be null if id is stored in json)
     * @return Number of updated datasets
     * @throws DynException 
     */
    public abstract Long update(String json, Long id) throws DynException;
    
    /**
     * Updates one dataset with the data given as json object
     * 
     * @param json Json Object containing data to update and identity attribute with value
     * @param id Id of the dataset to update (can be null if id is stored in json)
     * @return Number of updated datasets
     * @throws DynException 
     */
    public abstract Long update(JsonObject json, Long id) throws DynException;
    
    /**
     * Deletes an dataset
     * 
     * @param id Id of the dataset to delete
     * @return Id 
     * @throws DynException 
     */
    public abstract Long delete(String id) throws DynException;
}
