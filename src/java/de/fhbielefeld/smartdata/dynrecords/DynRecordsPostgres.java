package de.fhbielefeld.smartdata.dynrecords;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.smartdata.config.Configuration;
import de.fhbielefeld.smartdata.converter.DataConverter;
import de.fhbielefeld.smartdata.dbo.Attribute;
import de.fhbielefeld.smartdata.dyn.DynPostgres;
import de.fhbielefeld.smartdata.dynrecords.filter.Filter;
import de.fhbielefeld.smartdata.dynrecords.filter.FilterException;
import de.fhbielefeld.smartdata.dyncollection.DynCollectionPostgres;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.io.StringReader;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonValue;
import de.fhbielefeld.smartdata.dyncollection.DynCollection;

/**
 * Dynamic data access for postgres databases
 *
 * @author Florian Fehring
 */
public final class DynRecordsPostgres extends DynPostgres implements DynRecords {

    protected String schema;
    protected String table;

    protected DynCollection dyntable = null;
    protected static final Map<String, PreparedStatement> preparedStatements = new HashMap<>();
    protected static final Map<String, Map<String, Integer>> preparedPlaceholders = new HashMap<>();

    public DynRecordsPostgres(String schema, String table) throws DynException {
        this.schema = schema;
        this.table = table;

        // Get available columns
        this.dyntable = new DynCollectionPostgres(this.schema, this.table);
        this.connect();
    }

    @Override
    public String getPreparedQuery(String includes, Collection<Filter> filters, int size, String page, String order, boolean countOnly, String unique, boolean deflatt) throws DynException {
        // Build statement id string
        String stmtId = "";
        stmtId += this.schema + '_' + this.table;
        if (includes != null) {
            stmtId += includes;
        }
        if (filters != null) {
            //TODO pruefen ob das hier reiht oder nicht jedes mal ein neues Ergebnis gibt
            stmtId += filters.hashCode();
        }
        if (order != null) {
            stmtId += order;
        }
        if (size > 0) {
            stmtId += size;
        }
        if (page != null) {
            stmtId += page;
        }

        stmtId += countOnly;

        // Create sql statement
        if (!this.preparedStatements.containsKey(stmtId)) {
            // Get available attributes
            Map<String, Attribute> attributes = this.dyntable.getAttributes();

            // If there is no fetchedData expected do not request
            if (attributes.isEmpty()) {
                throw new DynException("The table >" + this.schema + '/' + this.table + "< has no columns. So there is no data to get.");
            }

            String orderby = order;
            String orderkind = "DESC";
            boolean orderByAvailable = false;
            // Parse order request
            if (order != null && order.contains(",")) {
                orderby = order.split(",")[0];
                orderkind = order.split(",")[1];
            }

            // Build up query
            StringBuilder sqlbuilder = new StringBuilder();
            // Build up list of placeholders
            Map<String, Integer> placeholders = new HashMap<>();
            int placeholderNo = 1;
            // Build up list of columns
            Collection<String> includeColumns = new ArrayList<>();
            if (countOnly) {
                sqlbuilder.append("SELECT COUNT(*) AS count ");
            } else {
                sqlbuilder.append("SELECT ");

                // Build up list of columns if no ones given
                if (includes == null || includes.isEmpty()) {
                    for (String curColumn : attributes.keySet()) {
                        includeColumns.add(curColumn);
                        if (orderby != null && curColumn.equals(orderby)) {
                            orderByAvailable = true;
                        }
                    }
                } else {
                    // Parse includes
                    includeColumns = Arrays.asList(includes.split(","));

                    // Build list of columns
                    Collection<String> newColumnNames = new ArrayList<>();

                    for (Attribute curColumn : attributes.values()) {
                        // Check and automatic add orderby column
                        if (orderby != null && curColumn.getName().equals(orderby)) {
                            newColumnNames.add(curColumn.getName());
                            orderByAvailable = true;
                            continue;
                        }
                        // Automatically add column if its a identity column
                        if (curColumn.isIdentity()) {
                            newColumnNames.add(curColumn.getName());
                            // Check if id was given
                            if (includes.contains(curColumn.getName())) {
                                Message msg = new Message("Requested identity column "
                                        + ">" + curColumn.getName() + "< will be delivered in every case. You do not "
                                        + "have to list it in your measurementnames calling "
                                        + "getData()", MessageLevel.INFO);
                                Logger.addDebugMessage(msg);
                            }
                        } else if (includeColumns.contains(curColumn.getName())) {
                            newColumnNames.add(curColumn.getName());
                        }
                    }

                    // Check if there are columns requested not available                    
                    for (String curColumnName : includeColumns) {
                        if (!newColumnNames.contains(curColumnName)) {
                            String msgstr = "There requested column >"
                                    + curColumnName + "< is not available.";
                            Message msg = new Message(msgstr, MessageLevel.WARNING);
                            Logger.addDebugMessage(msg);
                            this.warnings.add(msgstr);
                        }
                    }

                    includeColumns = newColumnNames;
                }

                String mnamesstr = String.join("\",\"", includeColumns);
                if (mnamesstr.contains("\"point\",")) {
                    String replacement = "ST_X(ST_TRANSFORM(point,4674)) point_lon, ST_Y(ST_TRANSFORM(point,4674)) point_lat";
                    mnamesstr = mnamesstr.replace("\"point\"", replacement);
                }

                // Create select names
                sqlbuilder.append("\"");
                sqlbuilder.append(mnamesstr);
                sqlbuilder.append("\"");
            }
            sqlbuilder.append(" FROM ");
            sqlbuilder.append(this.schema);
            sqlbuilder.append(".");
            sqlbuilder.append(this.table);

            if (filters != null && !filters.isEmpty()) {
                sqlbuilder.append(" WHERE ");
                int i = 0;
                for (Filter curFilter : filters) {
                    if (i > 0) {
                        sqlbuilder.append(" AND ");
                    }
                    String prepcode = curFilter.getPrepareCode();
                    placeholders.put(curFilter.getFiltercode(), placeholderNo);
                    curFilter.setFirstPlaceholder(placeholderNo);
                    placeholderNo += curFilter.getNumberOfPlaceholders();
                    sqlbuilder.append(prepcode);
                    i++;
                }
            }

            // Adding order by
            if (orderby != null && !orderby.isEmpty() && countOnly == false) {
                if (orderByAvailable) {
                    sqlbuilder.append(" ORDER BY \"").append(orderby).append("\"");
                    // Adding orderkind
                    sqlbuilder.append(" ").append(orderkind);
                } else {
                    String warningtxt = "The orderby field >"
                            + orderby + "< is not available in the dataset. Could not"
                            + " order. Data will be unordered.";
                    Message msg = new Message(warningtxt, MessageLevel.WARNING);
                    Logger.addDebugMessage(msg);
                    this.warnings.add(warningtxt);
                }
            }

            // Adding offset if given
            if (page != null) {
                size = 20;
                sqlbuilder.append(" OFFSET ?");
                placeholders.put("offset", placeholderNo++);
            }

            // Adding limit if given
            if (size > 0) {
                sqlbuilder.append(" LIMIT ?");
                placeholders.put("limit", placeholderNo++);
            }

            // Modify select statement for unique requests
            if (unique != null) {
                StringBuilder newsqlsb = new StringBuilder();
                newsqlsb.append("SELECT DISTINCT \"");
                newsqlsb.append(unique);
                newsqlsb.append("\" FROM (");
                newsqlsb.append(sqlbuilder);
                newsqlsb.append(") as aliastable");
                sqlbuilder = newsqlsb;
            }

            // Modify select statement for export as json
            if (!deflatt && unique != null) {
                StringBuilder newsqlsb = new StringBuilder();
                newsqlsb.append("SELECT json_agg(t.\"" + unique + "\") from (");
                newsqlsb.append(sqlbuilder.toString());
                newsqlsb.append(") as t");
                sqlbuilder = newsqlsb;
            }

            if (!deflatt) {
                StringBuilder newsqlsb = new StringBuilder();
                newsqlsb.append("SELECT json_strip_nulls(array_to_json(array_agg(row_to_json(t)))) AS json from (");
                newsqlsb.append(sqlbuilder.toString());
                newsqlsb.append(") t");
                sqlbuilder = newsqlsb;
            }

            String stmt = sqlbuilder.toString();
            Message msg = new Message("DynDataPostgres", MessageLevel.INFO, "SQL: " + stmt);
            Logger.addDebugMessage(msg);
            try {
                PreparedStatement pstmt = this.con.prepareStatement(stmt);
                this.preparedStatements.put(stmtId, pstmt);
                this.preparedPlaceholders.put(stmtId, placeholders);
            } catch (SQLException ex) {
                DynException dex = new DynException("Could not prepare staement >" + stmt + "<");
                dex.addSuppressed(ex);
            }
        }

        return stmtId;
    }

    @Override
    public PreparedStatement setQueryClauses(String stmtid, Collection<Filter> filters, int size, String page) throws DynException {
        PreparedStatement stmt = this.preparedStatements.get(stmtid);
        Map<String, Integer> placeholders = this.preparedPlaceholders.get(stmtid);

        for (Filter curFilter : filters) {
            try {
                stmt = curFilter.setFilterValue(stmt);
                this.warnings.addAll(curFilter.getWarnings());
            } catch (FilterException ex) {
                this.warnings.add("Filter >" + curFilter.getFiltercode() + "< could not be applied: " + ex.getLocalizedMessage());
            }
        }

        try {
            // Adding offset if given
            if (page != null) {
                int pageno = 1;
                size = 20;
                if (page.contains(",")) {
                    pageno = Integer.parseInt(page.split(",")[0]);
                    size = Integer.parseInt(page.split(",")[1]);
                } else {
                    pageno = Integer.parseInt(page);
                }
                int offsetpos = placeholders.get("offset");
                stmt.setInt(offsetpos, size * pageno - 1);
            }

            if (size > 0) {
                int limitpos = placeholders.get("limit");
                stmt.setInt(limitpos, size);
            }
        } catch (SQLException ex) {
            DynException de = new DynException("Could not execute query: " + ex.getLocalizedMessage());
            de.addSuppressed(ex);
            throw de;
        }

        return stmt;
    }

    @Override
    public String get(String includes, Collection<Filter> filters, int size, String page, String order, boolean countOnly, String unique, boolean deflatt) throws DynException {
        // Reset warnings for new get
        this.warnings = new ArrayList<>();

        Configuration conf = new Configuration();
        String hardLimitStr = conf.getProperty("hardLimit");
        if(hardLimitStr != null) {
            int hardLimit = Integer.parseInt(hardLimitStr);
            if(size == 0 || size > hardLimit)
                size = hardLimit;
            // Add warning message
            if(size > hardLimit) {
                this.warnings.add("The given limit of >" + size + "< exeeds the maximum of >" + hardLimit + "<. You will recive a maximum of >" + hardLimit + "< datasets.");
            }
        }
        
        try {
            // Prepare query or get allready prepeared one
            String stmtid = this.getPreparedQuery(includes, filters, size, page, order, countOnly, unique, deflatt);
            // Fill prepared query with data
            PreparedStatement pstmt = this.setQueryClauses(stmtid, filters, size, page);
            ResultSet rs = pstmt.executeQuery();
            String json = "{}";
            if (rs.next()) {
                String dbjson = rs.getString("json");
                if (dbjson != null && !dbjson.isEmpty()) {
                    json = dbjson;
                }
            }
            return json;
        } catch (SQLException ex) {
            DynException de = new DynException("Exception fetching data: " + ex.getLocalizedMessage());
            de.addSuppressed(ex);
            throw de;
        }
    }

    @Override
    public String getPreparedInsert(JsonObject json) throws DynException {
        String pstmtid = "insert_" + String.join("_", json.keySet());

        if (!this.preparedStatements.containsKey(pstmtid)) {
            Map<String, Attribute> columns = this.dyntable.getAttributes();
            Map<String, Integer> placeholders = new HashMap<>();

            // Build up insert statement
            StringBuilder sqlbuilder = new StringBuilder();
            sqlbuilder.append("INSERT INTO ");
            sqlbuilder.append(this.schema);
            sqlbuilder.append(".");
            sqlbuilder.append(this.table);
            sqlbuilder.append(" (\"");

            int foundCols = 0;
            for (String curKey : json.keySet()) {
                pstmtid += curKey;
                // Check if table expects that data
                if (!columns.containsKey(curKey)) {
                    continue;
                }
                if (foundCols > 0) {
                    sqlbuilder.append("\",\"");
                }
                sqlbuilder.append(curKey);

                foundCols++;
                // Note placeholder
                placeholders.put(curKey, foundCols);
            }
            sqlbuilder.append("\") VALUES (");
            for (int i = 0; i < foundCols; i++) {
                if (i > 0) {
                    sqlbuilder.append(",");
                }
                sqlbuilder.append("?");
            }
            sqlbuilder.append(")");

            String sql = sqlbuilder.toString();
            Message msg = new Message("DynDataPostgres/getPreparedInsert",MessageLevel.INFO,sql);
            Logger.addDebugMessage(msg);

            // Build up primary key query
            StringBuilder sqlbuilderid = new StringBuilder();
            sqlbuilderid.append("SELECT ");
            sqlbuilderid.append(" \"");

            int foundIds = 0;
            String firstidColumn = null;
            for (Attribute curColumn : columns.values()) {
                if (curColumn.isIdentity()) {
                    if (foundIds == 0) {
                        firstidColumn = curColumn.getName();
                    }
                    if (foundIds > 0) {
                        sqlbuilderid.append("\",\"");
                    }
                    sqlbuilderid.append(curColumn.getName());
                    foundIds++;
                }
            }

            sqlbuilderid.append("\" FROM ");
            sqlbuilderid.append(this.schema);
            sqlbuilderid.append(".");
            sqlbuilderid.append(this.table);
            sqlbuilderid.append(" ORDER BY ");
            sqlbuilderid.append(firstidColumn);
            sqlbuilderid.append(" DESC LIMIT 1");
            
            try {
                // Prepare statement
                PreparedStatement pstmt = this.con.prepareStatement(sql);
                this.preparedStatements.put(pstmtid, pstmt);
                this.preparedPlaceholders.put(pstmtid, placeholders);
                if (firstidColumn != null) {
                    String idsql = sqlbuilderid.toString();
                    PreparedStatement idpstmt = this.con.prepareStatement(idsql);
                    this.preparedStatements.put("id_" + pstmtid, idpstmt);
                }
            } catch (SQLException ex) {
                DynException de = new DynException("Could not prepare statement >"
                        + sql + "<: " + ex.getLocalizedMessage());
                de.addSuppressed(ex);
                throw de;
            }
        }
        return pstmtid;
    }

    @Override
    public Long create(String json) throws DynException {
        // Reset warnings for new create
        this.warnings = new ArrayList<>();
        JsonReader jsonReader = Json.createReader(new StringReader(json));
        // Single or array mode
        if(json.startsWith("[")) {
            JsonArray jsonarray = jsonReader.readArray();
            jsonReader.close();
            Long lastid = null;
            for(int i=0; i < jsonarray.size(); i++) {
                lastid = this.create(jsonarray.getJsonObject(i));
            }
            return lastid;
        } else {
            JsonObject jsonobject = jsonReader.readObject();
            jsonReader.close();
            return this.create(jsonobject);
        }
    }

    @Override
    public Long create(JsonObject json) throws DynException {
        String pstmtid = this.getPreparedInsert(json);
        PreparedStatement pstmt = this.preparedStatements.get(pstmtid);
        Map<String, Integer> placeholders = this.preparedPlaceholders.get(pstmtid);
        Map<String, Attribute> columns = this.dyntable.getAttributes();

        for (Map.Entry<String, JsonValue> curEntry : json.entrySet()) {
            String jkey = curEntry.getKey();
            // Check if table expects that data
            if (!placeholders.containsKey(jkey)) {
                this.warnings.add("Table >" + this.table + "< does not expect data for >" + jkey + "<");
                continue;
            }

            int pindex = placeholders.get(jkey);

            // Get column information
            Attribute curColumn = columns.get(jkey);
            this.setPlaceholder(pstmt,pindex,curColumn,curEntry.getValue());
        }
        try {
            pstmt.executeUpdate();
            // Request primary key
            PreparedStatement idpstmt = this.preparedStatements.get("id_" + pstmtid);
            if (idpstmt != null) {
                ResultSet prs = idpstmt.executeQuery();
                if (prs.next()) {
                    return prs.getLong(1);
                }
            }
            return null;
        } catch (SQLException ex) {
            DynException de = new DynException("Could not save dataset: " + ex.getLocalizedMessage());
            de.addSuppressed(ex);
            throw de;
        }
    }
    
    @Override
    public String getPreparedUpdate(JsonObject json, Long id) throws DynException {
        String pstmtid = "update_" + String.join("_", json.keySet());

        if (!this.preparedStatements.containsKey(pstmtid)) {
            Map<String, Attribute> columns = this.dyntable.getAttributes();
            Map<String, Integer> placeholders = new HashMap<>();

            // Build up insert statement
            StringBuilder sqlbuilder = new StringBuilder();
            sqlbuilder.append("UPDATE ");
            sqlbuilder.append(this.schema);
            sqlbuilder.append(".");
            sqlbuilder.append(this.table);
            sqlbuilder.append(" SET ");

            int foundCols = 0;
            String identitycol = null;
            for (String curKey : json.keySet()) {
                pstmtid += curKey;
                // Check if table expects that data
                if (!columns.containsKey(curKey)) {
                    continue;
                }
                // Check if column is identity column                
                if(columns.get(curKey).isIdentity()) {
                    identitycol = curKey;
                    continue;
                }
                if (foundCols > 0) {
                    sqlbuilder.append(",");
                }
                sqlbuilder.append("\"");
                sqlbuilder.append(curKey);
                sqlbuilder.append("\" = ?");

                // Note placeholder
                foundCols++;
                placeholders.put(curKey, foundCols);
            }

            if(identitycol == null && id != null) {
                // Autodetect identity column
                List<Attribute> idcolumns = this.dyntable.getIdentityAttributes();
                if(columns.isEmpty()) {
                    throw new DynException("There is no identity column in table. Could not update datasets.");
                }
                identitycol = idcolumns.get(0).getName();
            } else if(identitycol == null) {
                throw new DynException("There was no identity column given to identify the set to update.");
            }
            sqlbuilder.append(" WHERE ");
            sqlbuilder.append(identitycol);
            sqlbuilder.append(" = ?");
            foundCols++;
            placeholders.put(identitycol,foundCols);
            
            String sql = sqlbuilder.toString();
            Message msg = new Message("DynDataPostgres/getPreparedUpdate",MessageLevel.INFO,sql);
            Logger.addDebugMessage(msg);
            
            try {
                // Prepare statement
                PreparedStatement pstmt = this.con.prepareStatement(sql);
                this.preparedStatements.put(pstmtid, pstmt);
                this.preparedPlaceholders.put(pstmtid, placeholders);
            } catch (SQLException ex) {
                DynException de = new DynException("Could not prepare statement >"
                        + sql + "<: " + ex.getLocalizedMessage());
                de.addSuppressed(ex);
                throw de;
            }
        }
        return pstmtid;
    }
    
    @Override
    public Long update(String json, Long id) throws DynException {
        // Reset warnings for new create
        this.warnings = new ArrayList<>();
        JsonReader jsonReader = Json.createReader(new StringReader(json));
        // Detect array content
        JsonArray jsonarray;
        if(json.startsWith("[")) {
            jsonarray = jsonReader.readArray();
            jsonReader.close();
        } else if(json.contains("\"records\"")) {
            JsonObject jsonobject = jsonReader.readObject();
            jsonReader.close();
            jsonarray = jsonobject.getJsonArray("records");
        } else if(json.contains("\"list\"")) {
            JsonObject jsonobject = jsonReader.readObject();
            jsonReader.close();
            jsonarray = jsonobject.getJsonArray("list");
        } else {
            JsonObject jsonobject = jsonReader.readObject();
            jsonReader.close();            
            return this.update(jsonobject,id);
        }
        
        Long lastid = null;
        for(int i=0; i < jsonarray.size(); i++) {
            lastid = this.update(jsonarray.getJsonObject(i), null);
        }
        return lastid;
    }
    
    @Override
    public Long update(JsonObject json, Long id) throws DynException {
        String pstmtid = this.getPreparedUpdate(json, id);
        PreparedStatement pstmt = this.preparedStatements.get(pstmtid);
        Map<String, Integer> placeholders = this.preparedPlaceholders.get(pstmtid);
        Map<String, Attribute> columns = this.dyntable.getAttributes();
    
        int usedPlaceholders = 1;
        for (Map.Entry<String, JsonValue> curEntry : json.entrySet()) {
            String jkey = curEntry.getKey();
            // Check if table expects that data
            if (!placeholders.containsKey(jkey)) {
                this.warnings.add("Table >" + this.table + "< does not expect data for >" + jkey + "<");
                continue;
            }

            int pindex = placeholders.get(jkey);

            // Get column information
            Attribute curColumn = columns.get(jkey);
            this.setPlaceholder(pstmt,pindex,curColumn,curEntry.getValue());
            usedPlaceholders++;
        }

        // If there is a placeholder left it will be the id
        if(usedPlaceholders <= placeholders.size()) {
            try {
                int nextId = usedPlaceholders++;
                pstmt.setLong(nextId, id);
            } catch(SQLException ex) {
                DynException de = new DynException("Could set id to update statement: " + ex.getLocalizedMessage());
                de.addSuppressed(ex);
                throw de;
            }
        }
        
        try {
            pstmt.executeUpdate();
            // Request primary key
            PreparedStatement idpstmt = this.preparedStatements.get("id_" + pstmtid);
            if (idpstmt != null) {
                ResultSet prs = idpstmt.executeQuery();
                if (prs.next()) {
                    return prs.getLong(1);
                }
            }
            return null;
        } catch (SQLException ex) {
            DynException de = new DynException("Could not update dataset: " + ex.getLocalizedMessage());
            de.addSuppressed(ex);
            throw de;
        }
    }
    
    /**
     * Set values to a placeholder
     * 
     * @param pstmt Prepared statement where to set the value
     * @param pindex Index of the parameter to set
     * @param col Parameters column
     * @param value Parameters value
     * @throws DynException 
     */
    private void setPlaceholder(PreparedStatement pstmt, int pindex, Attribute col, JsonValue value) throws DynException {
        try {
                switch (col.getType()) {
                    case "text":
                    case "varchar":
                        JsonString jstr = (JsonString) value;
                        pstmt.setString(pindex, jstr.getString());
                        break;
                    case "boolean":
                        // Isn't there a better method to get the boolean value?
                        boolean bool = Boolean.parseBoolean(value.toString());
                        pstmt.setBoolean(pindex, bool);
                        break;
                    case "float4":
                    case "float8":
                        JsonNumber jdoub = (JsonNumber) value;
                        pstmt.setDouble(pindex, jdoub.doubleValue());
                        break;
                    case "int2":
                    case "int4":
                        JsonNumber jint = (JsonNumber) value;
                        pstmt.setInt(pindex, jint.intValue());
                        break;
                    case "int8":
                        JsonNumber jbint = (JsonNumber) value;
                        pstmt.setLong(pindex, jbint.longValue());
                        break;
                    case "timestamp":
                        JsonString jts = (JsonString) value;
                        LocalDateTime ldt = DataConverter.objectToLocalDateTime(jts.getString());
                        pstmt.setTimestamp(pindex, Timestamp.valueOf(ldt));
                        break;
                    default:
                        Message msg = new Message(
                                "DynDataPostgres", MessageLevel.WARNING,
                                "Write to database does not support type >" + col.getType() + "<");
                        Logger.addDebugMessage(msg);
                        this.warnings.add("Could not save value for >" + col.getName() + "<: Datatype >" + col.getType() + "< is not supported.");
                }
            } catch (SQLException ex) {
                this.warnings.add("Could not save value for >" + col.getName() + "<: " + ex.getLocalizedMessage());
            }
    }
    
    @Override
    public Long delete(String idstr) throws DynException {
        String[] ids = idstr.split(",");
        String sql = "DELETE FROM " + this.schema + "." + this.table + " WHERE ";
        
        // Get name of first id column
        List<Attribute> columns = this.dyntable.getIdentityAttributes();
        if(columns.isEmpty()) {
            throw new DynException("Could not delete from >" + this.schema + "." + this.table + " because there is no identity column.");
        }
        // Get first column
        Attribute idcol = columns.get(0);
        
        sql += "\"" + idcol.getName() + "\" = ";
        if(idcol.getType().equalsIgnoreCase("varchar")) {
            sql += "\"";
            sql += String.join("\" OR \"" + idcol.getName() + "\" = \"", ids);
            sql += "\"";
        } else {
            sql += String.join(" OR \"" + idcol.getName() + "\" = ", ids);
        }
        
        try {
            Statement stmt = this.con.createStatement();
            stmt.executeUpdate(sql);
            return null;
        } catch (SQLException ex) {
            DynException de = new DynException("Could not update dataset: " + ex.getLocalizedMessage());
            de.addSuppressed(ex);
            throw de;
        }
    }
}