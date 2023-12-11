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
import de.fhbielefeld.smartdata.dyncollection.DynCollection;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonNumber;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonReader;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;
import jakarta.json.JsonValue.ValueType;
import jakarta.json.JsonWriter;
import jakarta.json.JsonWriterFactory;
import jakarta.json.stream.JsonGenerator;
import jakarta.json.stream.JsonParser;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.sql.Date;
import java.util.Map.Entry;

/**
 * Dynamic data access for postgres databases
 *
 * @author Florian Fehring
 */
public final class DynRecordsPostgres extends DynPostgres implements DynRecords {

    protected String schema;
    protected String table;
    protected String lastStmtId = null;

    protected DynCollection dyncollection = null;
    protected static final Map<String, String> preparedStatements = new HashMap<>();
    protected static final Map<String, Map<String, Integer>> preparedPlaceholders = new HashMap<>();
    protected static final Map<String, List<String>> preparedWarnings = new HashMap<>();
    protected static final Map<String, DynCollection> usedDynCollections = new HashMap<>();

    public DynRecordsPostgres(String schema, String table) throws DynException {
        this.schema = schema;
        this.table = table;
        this.connect();

        // Get available columns
        this.dyncollection = new DynCollectionPostgres(this.schema, this.table, this.con);
    }

    @Override
    public String getPreparedQuery(String includes, Collection<Filter> filters, int size, String page, String order, boolean countOnly, String unique, boolean deflatt, String geojsonattr, String geotransform, Collection<String> joins) throws DynException {
        // Build statement id string
        String stmtId = "";
        stmtId += this.schema + '_' + this.table;
        if (includes != null) {
            stmtId += includes;
        }
        if (filters != null) {
            for (Filter curFilter : filters) {
                stmtId += curFilter.getPrepareCode();
            }
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
        if (geojsonattr != null) {
            stmtId += "_geo" + geojsonattr;
        }
        if (geotransform != null) {
            stmtId += "_geot" + geotransform;
        }
        if (joins != null) {
            stmtId += joins;
        }

        stmtId += countOnly;
        this.lastStmtId = stmtId;

        // Create sql statement
        if (!this.preparedStatements.containsKey(stmtId)) {
            this.preparedWarnings.put(stmtId, new ArrayList<>());
            // Get available attributes
            Map<String, Attribute> attributes = this.dyncollection.getAttributes();

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
            StringBuilder selectbuilder = new StringBuilder();
            // Build up list of placeholders
            Map<String, Integer> placeholders = new HashMap<>();
            int placeholderNo = 1;

            if (countOnly) {
                selectbuilder.append("SELECT COUNT(*) AS count ");
            } else {
                selectbuilder.append("SELECT ");

                // Parse and split requested attribute names
                boolean attrsSelected = false;
                Collection<String> requestedAttr = new ArrayList<>();
                if (includes != null && !includes.isEmpty()) {
                    // Parse includes
                    requestedAttr = new ArrayList<>(Arrays.asList(includes.split(",")));
                    attrsSelected = true;
                }

                // Build list of columns
                ArrayList<String> queryColExpressions = new ArrayList<>();

                for (Attribute curColumn : attributes.values()) {
                    // Automatically add column if its a identity column
                    if (curColumn.isIdentity()) {
                        // Check if id was given
                        if (requestedAttr.contains(curColumn.getName())) {
                            Message msg = new Message("Requested identity attribute "
                                    + ">" + curColumn.getName() + "< will be delivered in every case. You do not "
                                    + "have to list it in your measurementnames calling "
                                    + "getData()", MessageLevel.INFO);
                            Logger.addDebugMessage(msg);
                        } else if (attrsSelected) {
                            requestedAttr.add(curColumn.getName());
                        }
                    }

                    // Check and automatic add orderby column
                    if (orderby != null && curColumn.getName().equals(orderby)) {
                        orderByAvailable = true;
                        // Check if orderby column was given
                        if (requestedAttr.contains(curColumn.getName())) {
                            Message msg = new Message("Requested orderby attribute "
                                    + ">" + curColumn.getName() + "< will be delivered in every case. You do not "
                                    + "have to list it in your measurementnames calling "
                                    + "getData()", MessageLevel.INFO);
                            Logger.addDebugMessage(msg);
                        } else if (attrsSelected) {
                            requestedAttr.add(curColumn.getName());
                        }
                    }

                    // Go next if attribute is not requested
                    if (attrsSelected && !requestedAttr.contains(curColumn.getName())) {
                        continue;
                    }

                    // Exclude geo attribute, if geojson should be returned
                    if (geojsonattr != null && curColumn.getName().equals(geojsonattr)) {
                        requestedAttr.remove(geojsonattr);
                        continue;
                    }

                    // Create selection expression
                    if (curColumn.getType().equalsIgnoreCase("bytea")) {
                        // binary data should be fetched base64 encoded
                        queryColExpressions.add("ENCODE(\"" + curColumn.getName() + "\", 'BASE64') " + curColumn.getName());
                    } else if (curColumn.getType().equalsIgnoreCase("geometry") && geotransform != null) {
                        // Convert to latlon output
                        if (geotransform.equalsIgnoreCase("latlon")) {
                            queryColExpressions.add("ST_X(ST_TRANSFORM(\""
                                    + curColumn.getName() + "\",4674)) "
                                    + curColumn.getName() + "_lon, ST_Y(ST_TRANSFORM(\""
                                    + curColumn.getName() + "\",4674)) " + curColumn.getName() + "_lat");
                        } else {
                            // Treat as EPSG code
                            queryColExpressions.add("ST_TRANSFORM(\""
                                    + curColumn.getName() + "\"," + geotransform + ") " + curColumn.getName());
                        }
                    } else {
                        queryColExpressions.add("\"" + this.table + "\".\"" + curColumn.getName() + "\"");
                    }
                    // Remove from requesteds list
                    requestedAttr.remove(curColumn.getName());
                }

                // Check if there are columns requested not available    
                if (!requestedAttr.isEmpty()) {
                    String msgstr = "There requested columns >"
                            + requestedAttr + "< are not available.";
                    Message msg = new Message(msgstr, MessageLevel.WARNING);
                    Logger.addDebugMessage(msg);
                    this.preparedWarnings.get(stmtId).add(msgstr);
                }

                String namesstr = String.join(",", queryColExpressions);
                // Create select names
                selectbuilder.append(namesstr);
            }

            // Build FROM ... WHERE clause (separate because in geojson request it must be placed on other location)
            StringBuilder frombuilder = new StringBuilder();

            frombuilder.append(" FROM ");
            frombuilder.append("\"");
            frombuilder.append(this.schema);
            frombuilder.append("\"");
            frombuilder.append(".");
            frombuilder.append("\"");
            frombuilder.append(this.table);
            frombuilder.append("\"");

            // Create join
            if (joins != null && !joins.isEmpty()) {
                for (String curJoins : joins) {
                    String[] curJoinCols = curJoins.split(",");
                    // Notice last collection (this one is where we want data from)
                    String lastCol = this.table;
                    DynCollection lastDynCol = this.dyncollection;
                    DynCollection sc = null;
                    // Get collection that should joined
                    for (String curJoinCol : curJoinCols) {
                        // Get information about second collection
                        sc = this.usedDynCollections.get(curJoinCol);
                        if (sc == null) {
                            sc = new DynCollectionPostgres(this.schema, curJoinCol);
                            this.usedDynCollections.put(curJoinCol, sc);
                        }

                        Attribute refAttr = lastDynCol.getReferenceTo(curJoinCol);
                        String fromCol;
                        String fromAttr;
                        String toCol;
                        String toAttr;
                        // If from main collection is no connection to second collection
                        if (refAttr == null) {
                            // Search connection on first collection
                            refAttr = sc.getReferenceTo(lastCol);
                            if (refAttr == null) {
                                this.preparedWarnings.get(stmtId).add("Could not join >" + lastCol + "< with >" + curJoinCol + "< there is no binding attribute.");
                                continue;
                            } else {
                                fromCol = curJoinCol;
                                fromAttr = refAttr.getName();
                                toCol = lastCol;
                                toAttr = refAttr.getRefAttribute();
                            }
                        } else {
                            fromCol = lastCol;
                            fromAttr = refAttr.getName();
                            toCol = curJoinCol;
                            toAttr = refAttr.getRefAttribute();
                        }
                        frombuilder.append(" INNER JOIN \"");
                        frombuilder.append(this.schema);
                        frombuilder.append("\"");
                        frombuilder.append(".");
                        frombuilder.append("\"");
                        frombuilder.append(curJoinCol);
                        frombuilder.append("\" ON \"");
                        frombuilder.append(fromCol);
                        frombuilder.append("\".\"");
                        frombuilder.append(fromAttr);
                        frombuilder.append("\" = \"");
                        frombuilder.append(toCol);
                        frombuilder.append("\".\"");
                        frombuilder.append(toAttr);
                        frombuilder.append("\"");

                        lastCol = curJoinCol;
                        lastDynCol = sc;
                    }

                    // group by for sub joins
                    frombuilder.append(" GROUP BY ");
                    frombuilder.append("\"");
                    frombuilder.append(this.table);
                    frombuilder.append("\".\"");
                    frombuilder.append(sc.getIdentityAttributes().get(0).getName());
                    frombuilder.append("\"");

                    // Create select names for joined tables
                    StringBuilder subSelectBuilder = new StringBuilder();
                    subSelectBuilder.append(", json_agg(json_build_object(");
                    int i = 0;
                    for (Attribute curAttr : sc.getAttributes().values()) {
                        if (i > 0) {
                            subSelectBuilder.append(", ");
                        }
                        subSelectBuilder.append("'");
                        subSelectBuilder.append(curAttr.getName());
                        subSelectBuilder.append("', ");
                        subSelectBuilder.append("\"");
                        subSelectBuilder.append(lastCol);
                        subSelectBuilder.append("\".\"");
                        subSelectBuilder.append(curAttr.getName());
                        subSelectBuilder.append("\"");
                        i++;
                    }
                    subSelectBuilder.append(")) as ");
                    subSelectBuilder.append(lastCol);
                    selectbuilder.append(subSelectBuilder);
                }
            }

            if (filters != null && !filters.isEmpty()) {
                frombuilder.append(" WHERE ");
                int i = 0;
                for (Filter curFilter : filters) {
                    if (i > 0) {
                        frombuilder.append(" AND ");
                    }
                    String prepcode = curFilter.getPrepareCode();
                    placeholders.put(curFilter.getPrepareCode(), placeholderNo);
                    placeholderNo += curFilter.getNumberOfPlaceholders();
                    frombuilder.append(prepcode);
                    i++;
                }
            }

            // Adding order by
            if (orderby != null && !orderby.isEmpty() && countOnly == false) {
                if (orderByAvailable) {
                    frombuilder.append(" ORDER BY \"").append(orderby).append("\"");
                    // Adding orderkind
                    frombuilder.append(" ").append(orderkind);
                } else {
                    String warningtxt = "The orderby field >"
                            + orderby + "< is not available in the dataset. Could not"
                            + " order. Data will be unordered.";
                    Message msg = new Message(warningtxt, MessageLevel.WARNING);
                    Logger.addDebugMessage(msg);
                    this.preparedWarnings.get(stmtId).add(warningtxt);
                }
            }

            // Adding offset if given
            if (page != null) {
                if (size < 1) {
                    size = 20;
                }
                frombuilder.append(" OFFSET ?");
                placeholders.put("offset", placeholderNo++);
            }

            // Adding limit if given
            if (size > 0) {
                frombuilder.append(" LIMIT ?");
                placeholders.put("limit", placeholderNo++);
            }

            // Modify select statement for unique requests
            if (unique != null) {
                StringBuilder newsqlsb = new StringBuilder();
                newsqlsb.append("SELECT DISTINCT \"");
                newsqlsb.append(unique);
                newsqlsb.append("\" FROM (");
                newsqlsb.append(selectbuilder);
                newsqlsb.append(") as aliastable");
                selectbuilder = newsqlsb;
            }

            if (geojsonattr == null) {
                selectbuilder.append(frombuilder);
            }

            String prespecsql = selectbuilder.toString();

            // Modify select statement for export as json
            if (unique != null) {
                StringBuilder newsqlsb = new StringBuilder();
                newsqlsb.append("SELECT json_agg(t.\"" + unique + "\") from (");
                newsqlsb.append(selectbuilder.toString());
                newsqlsb.append(") as t");
                selectbuilder = newsqlsb;
            }

            // Remove null values
            StringBuilder rnullsqlsb = new StringBuilder();
            rnullsqlsb.append("SELECT json_strip_nulls(array_to_json(array_agg(row_to_json(t)))) AS json from (");
            rnullsqlsb.append(prespecsql);
            rnullsqlsb.append(") t");
            selectbuilder = rnullsqlsb;

            if (geojsonattr != null) {
                // Get geojsonattr information
                Attribute geoattr = attributes.get(geojsonattr);
                StringBuilder newsqlsb = new StringBuilder();
                // Package into one json
                newsqlsb.append("SELECT row_to_json(fc) AS json FROM (");
                // Create feature collection
                newsqlsb.append("SELECT 'FeatureCollection' AS type, array_to_json(array_agg(f)) AS features  FROM (");
                // Add type: "feature"
                newsqlsb.append("SELECT 'Feature' AS type");
                // Add geometry information
                newsqlsb.append(", ST_AsGeoJSON(");
                // Add transformation if needed
                if (geoattr.getDimension() == 2 && geoattr.getSrid() != 4326) {
                    newsqlsb.append("ST_Transform(");
                }
                if (geoattr.getDimension() == 3 && geoattr.getSrid() != 4979) {
                    newsqlsb.append("ST_Transform(");
                }
                newsqlsb.append("\"");
                newsqlsb.append(geojsonattr);
                newsqlsb.append("\"");

                // Add transformation if needed
                if (geoattr.getDimension() == 2 && geoattr.getSrid() != 4326) {
                    newsqlsb.append(",4326)");
                }
                if (geoattr.getDimension() == 3 && geoattr.getSrid() != 4979) {
                    newsqlsb.append(",4979)");
                }

                newsqlsb.append(")::json as geometry");

                // Add properties attribute
                newsqlsb.append(", (");
                // Filter null values
                newsqlsb.append("SELECT json_strip_nulls(row_to_json(t)) FROM (");
                newsqlsb.append(prespecsql);
                newsqlsb.append(") AS t");
                newsqlsb.append(") AS properties");
                newsqlsb.append(frombuilder);
                newsqlsb.append(") AS f");
                newsqlsb.append(") AS fc");
                selectbuilder = newsqlsb;
            }

            String stmt = selectbuilder.toString();
            Message msg = new Message("SQL: " + stmt, MessageLevel.INFO);
            Logger.addDebugMessage(msg);
            this.preparedStatements.put(stmtId, stmt);
            this.preparedPlaceholders.put(stmtId, placeholders);
        }

        return stmtId;
    }

    @Override
    public PreparedStatement setQueryClauses(String stmtid, Collection<Filter> filters, int size, String page) throws DynException {
        String stmt = this.preparedStatements.get(stmtid);
        Map<String, Integer> placeholders = this.preparedPlaceholders.get(stmtid);

        try {
            PreparedStatement pstmt = this.con.prepareStatement(stmt);

            for (Filter curFilter : filters) {
                try {
                    Integer placeholderpos = placeholders.get(curFilter.getPrepareCode());
                    curFilter.setFirstPlaceholder(placeholderpos);
                    pstmt = curFilter.setFilterValue(pstmt);
                    this.warnings.addAll(curFilter.getWarnings());
                } catch (FilterException ex) {
                    this.warnings.add("Filter >" + curFilter.getFiltercode() + "< could not be applied: " + ex.getLocalizedMessage().replaceAll("[\\r\\n]", ""));
                }
            }

            // Adding offset if given
            if (page != null) {
                int pageno;
                if (size < 1) {
                    size = 20;
                }
                if (page.contains(",")) {
                    pageno = Integer.parseInt(page.split(",")[0]);
                    size = Integer.parseInt(page.split(",")[1]);
                } else {
                    pageno = Integer.parseInt(page);
                }
                int offsetpos = placeholders.get("offset");
                pstmt.setInt(offsetpos, size * pageno - size);
            }

            if (size > 0) {
                int limitpos = placeholders.get("limit");
                pstmt.setInt(limitpos, size);
            }
            return pstmt;
        } catch (SQLException ex) {
            DynException de = new DynException("Could not execute query: " + ex.getLocalizedMessage().replaceAll("[\\r\\n]", ""));
            de.addSuppressed(ex);
            throw de;
        }
    }

    @Override
    public String get(String includes, Collection<Filter> filters, int size, String page, String order, boolean countOnly, String unique, boolean deflatt, String geojsonattr, String geotransform, Collection<String> joins) throws DynException {
        // Reset warnings for new get
        this.warnings = new ArrayList<>();

        Configuration conf = new Configuration();
        String hardLimitStr = conf.getProperty("hardLimit");
        if (hardLimitStr != null) {
            int hardLimit = Integer.parseInt(hardLimitStr);
            if (size <= 0 || size > hardLimit) {
                size = hardLimit;
            }
            // Add warning message
            if (size > hardLimit) {
                this.warnings.add("The given limit of >" + size + "< exeeds the maximum of >" + hardLimit + "<. You will recive a maximum of >" + hardLimit + "< datasets.");
            }
        }
        // Prepare query or get allready prepeared one
        String stmtid = this.getPreparedQuery(includes, filters, size, page, order, countOnly, unique, deflatt, geojsonattr, geotransform, joins);
        // Fill prepared query with data
        try (PreparedStatement pstmt = this.setQueryClauses(stmtid, filters, size, page); ResultSet rs = pstmt.executeQuery()) {
            String json = "{}";
            if (rs.next()) {
                String dbjson = rs.getString("json");
                if (dbjson != null && !dbjson.isEmpty()) {
                    json = dbjson;
                    if (deflatt) {
                        json = this.deflatt(json);
                    }
                }
            }
            rs.close();
            return json;
        } catch (SQLException ex) {
            DynException de = new DynException("SQL error fetching data: " + ex.getLocalizedMessage().replaceAll("\\p{Cc}", ""));
            de.addSuppressed(ex);
            ex.printStackTrace();
            throw de;
        } catch (Exception ex) {
            if (ex.getLocalizedMessage().contains("connection has been closed")) {
                // Try reconnect
                this.connect();
                return this.get(includes, filters, size, page, order, countOnly, unique, deflatt, geojsonattr, geotransform, joins);
            }
            DynException de = new DynException("Exception fetching data: " + ex.getLocalizedMessage().replaceAll("\\p{Cc}", ""));
            de.addSuppressed(ex);
            throw de;
        }
    }

    /**
     * Creates deflatted json representation of flatted datasets
     *
     * @return
     */
    private String deflatt(String json) {
        Map<Integer, Map<String, JsonValue>> newdatamap = new HashMap<>();
        // Parse json
        JsonParser parser = Json.createParser(new StringReader(json));
        parser.next();
        JsonArray dataArr = parser.getArray();
        for (JsonValue curVal : dataArr) {
            JsonObject curDataset = (JsonObject) curVal;
            for (Entry<String, JsonValue> curAttr : curDataset.entrySet()) {
                String attrcall = curAttr.getKey();
                // Get last digit position from the end
                int i = attrcall.length();
                while (i > 0 && Character.isDigit(attrcall.charAt(i - 1))) {
                    i--;
                }
                // When there is a digit at the end
                if (i != attrcall.length()) {
                    String attrname = attrcall.substring(0, i);
                    int deflattedId = Integer.parseInt(attrcall.substring(i));
                    // Create place for dataset in map if not exists
                    if (!newdatamap.containsKey(deflattedId)) {
                        newdatamap.put(deflattedId, new HashMap<>());
                    }
                    Map<String, JsonValue> setmap = newdatamap.get(deflattedId);
                    setmap.put(attrname, curAttr.getValue());
                }
            }
        }

        // New json array
        JsonArrayBuilder newdataarr = Json.createArrayBuilder();
        for (Entry<Integer, Map<String, JsonValue>> curVal : newdatamap.entrySet()) {
            JsonObjectBuilder newdataset = Json.createObjectBuilder();
            newdataset.add("id", curVal.getKey());
            for (Entry<String, JsonValue> curAttr : curVal.getValue().entrySet()) {
                newdataset.add(curAttr.getKey(), curAttr.getValue());
            }
            newdataarr.add(newdataset);
        }

        Map<String, Object> properties = new HashMap<>(1);
        properties.put(JsonGenerator.PRETTY_PRINTING, false);
        JsonWriterFactory writerFactory = Json.createWriterFactory(properties);
        StringWriter sw = new StringWriter();
        try (JsonWriter jsonWriter = writerFactory.createWriter(sw)) {
            jsonWriter.writeArray(newdataarr.build());
        }
        return sw.toString();
    }

    @Override
    public String getPreparedInsert(JsonObject json) throws DynException {
        String pstmtid = "insert_" + String.join("_", json.keySet());
        this.lastStmtId = pstmtid;

        if (!this.preparedStatements.containsKey(pstmtid)) {
            this.preparedWarnings.put(pstmtid, new ArrayList<>());
            Map<String, Attribute> columns = this.dyncollection.getAttributes();
            Map<String, Integer> placeholders = new HashMap<>();

            // Build up insert statement
            StringBuilder sqlbuilder = new StringBuilder();
            sqlbuilder.append("INSERT INTO ");
            sqlbuilder.append("\"");
            sqlbuilder.append(this.schema);
            sqlbuilder.append("\"");
            sqlbuilder.append(".");
            sqlbuilder.append("\"");
            sqlbuilder.append(this.table);
            sqlbuilder.append("\"");
            sqlbuilder.append(" (\"");

            StringBuilder colsstr = new StringBuilder();
            StringBuilder valuestr = new StringBuilder();
            int foundCols = 0;
            for (String curKey : json.keySet()) {
                pstmtid += curKey;
                // Check if table expects that data
                if (!columns.containsKey(curKey)) {
                    continue;
                }
                // Get definition for current column
                Attribute attr = columns.get(curKey);

                if (foundCols > 0) {
                    colsstr.append("\",\"");
                    valuestr.append(",");
                }
                colsstr.append(curKey);

                // Add placeholder depending on type
                switch (attr.getType()) {
                    case "json":
                        valuestr.append("to_json(?::json)");
                        break;
                    case "geometry":
                        valuestr.append("ST_GeomFromText(?)");
                        break;
                    default:
                        valuestr.append("?");
                }

                foundCols++;
                // Note placeholder
                placeholders.put(curKey, foundCols);
            }
            // Check if there is no data to insert
            if (foundCols < 1) {
                throw new DynException("There is no data to insert");
            }

            // Put together
            sqlbuilder.append(colsstr);
            sqlbuilder.append("\") VALUES (");
            sqlbuilder.append(valuestr);
            sqlbuilder.append(")");

            String sql = sqlbuilder.toString();
            Message msg = new Message("SQL: " + sql, MessageLevel.INFO);
            Logger.addDebugMessage(msg);

            this.preparedStatements.put(pstmtid, sql);
            this.preparedPlaceholders.put(pstmtid, placeholders);

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

            if (firstidColumn != null) {
                sqlbuilderid.append("\" FROM ");
                sqlbuilderid.append("\"");
                sqlbuilderid.append(this.schema);
                sqlbuilderid.append("\"");
                sqlbuilderid.append(".");
                sqlbuilderid.append("\"");
                sqlbuilderid.append(this.table);
                sqlbuilderid.append("\"");
                sqlbuilderid.append(" ORDER BY ");
                sqlbuilderid.append("\"");
                sqlbuilderid.append(firstidColumn);
                sqlbuilderid.append("\"");
                sqlbuilderid.append(" DESC LIMIT 1");
                String idsql = sqlbuilderid.toString();
                this.preparedStatements.put("id_" + pstmtid, idsql);
            }
        }
        return pstmtid;
    }

    @Override
    public List<Object> create(String json) throws DynException {
        // Reset warnings for new create
        this.warnings = new ArrayList<>();
        JsonReader jsonReader = Json.createReader(new StringReader(json));
        List<Object> ids = new ArrayList<>();
        // Records, geojson, array or Single mode
        if (json.startsWith("{\"type\":\"FeatureCollection\"")) {
            JsonObject jsonobject = jsonReader.readObject();
            jsonReader.close();
            JsonArray featurearray = jsonobject.getJsonArray("features");
            for (int i = 0; i < featurearray.size(); i++) {
                ids.add(this.createFromGeojson(featurearray.getJsonObject(i)));
            }
        } else if (json.startsWith("{\"records\":")) {
            JsonObject jsonobject = jsonReader.readObject();
            jsonReader.close();
            JsonArray jsonarray = jsonobject.getJsonArray("records");
            for (int i = 0; i < jsonarray.size(); i++) {
                ids.add(this.create(jsonarray.getJsonObject(i)));
            }
        } else if (json.startsWith("[")) {
            JsonArray jsonarray = jsonReader.readArray();
            jsonReader.close();
            for (int i = 0; i < jsonarray.size(); i++) {
                ids.add(this.create(jsonarray.getJsonObject(i)));
            }
        } else {
            JsonObject jsonobject = jsonReader.readObject();
            jsonReader.close();
            ids.add(this.create(jsonobject));
        }
        return ids;
    }

    /**
     * Converts a geosjon Feature into one simple json dataset, converting geo
     * informations into WKT.
     *
     * @param geojsonobj Geojson Feature object
     * @return Result of the insertion
     * @throws DynException
     */
    public Object createFromGeojson(JsonObject geojsonobj) throws DynException {
        JsonObjectBuilder databuilder = Json.createObjectBuilder();
        // Check if geojson is valid
        String featurestring = geojsonobj.getString("type");
        if (featurestring == null || !featurestring.equals("Feature")) {
            throw new DynException("Given json is not valid geojson");
        }
        JsonObject geom = geojsonobj.getJsonObject("geometry");
        // Check if there is a geometry
        if (geom == null) {
            throw new DynException("Given json has no geometry information");
        }
        // Check if geometry has no type
        if (geom.getString("type") == null) {
            throw new DynException("Given geometry has no type");
        }

        List<JsonObject> geometries = new ArrayList<>();
        // Check if geometry is collection
        if (geom.getString("type").equals("GeometryCollection")) {
            // Get all geometries
            for (JsonValue curGeom : geom.getJsonArray("geometries")) {
                geometries.add(curGeom.asJsonObject());
            }
        } else {
            // If its not a collection its a single geometry (of any kind)
            geometries.add(geom);
        }

        // Add all geometrie informations to geometry fields in given order (because there is no nameing mechanism in geojson)
        for (JsonValue curGeom : geometries) {
            // Search matching attribute
            for (Attribute curAttr : this.dyncollection.getGeoAttributes()) {
                // Create WKT from geojson geometty
                String wktsql = "SELECT ST_AsText(";
                // Adding coordinate system transformation if its different
                if (curAttr.getSrid() != 4326) {
                    wktsql += "ST_Transform(";
                }
                // Check if geometry type is matching
                if (!curGeom.asJsonObject().getString("type").equalsIgnoreCase(curAttr.getSubtype())) {
                    continue;
                }

                wktsql += "ST_GeomFromGeoJSON('" + curGeom.toString() + "')";
                if (curAttr.getSrid() != 4326) {
                    wktsql += "," + curAttr.getSrid() + ")";
                }
                wktsql += ") AS geom";

                String wkt;
                try (Statement wktstmt = this.con.createStatement(); ResultSet wktrs = wktstmt.executeQuery(wktsql)) {
                    wktrs.next();
                    // No other reference system awaited from geojson (removed from geojson specification)
                    wkt = "SRID=" + curAttr.getSrid() + ";" + wktrs.getString("geom");
                    // Add WKT to import object
                    databuilder.add(curAttr.getName(), wkt);
                    break;
                } catch (SQLException ex) {
                    String msg = "Could not get WKT from geojson: " + ex.getLocalizedMessage();
                    if (ex.getLocalizedMessage().contains("proj_crs_get_coordinate_system returned NULL")) {
                        msg = "Given coordinates and target coordinate system are not compatible";
                    }
                    this.warnings.add(msg);
                }
            }
        }

        JsonObject props = geojsonobj.getJsonObject("properties");
        if (props != null) {
            for (Entry<String, JsonValue> curProp : props.entrySet()) {
                databuilder.add(curProp.getKey(), curProp.getValue());
            }
        }

        return this.create(databuilder.build());
    }

    @Override
    public Object create(JsonObject json) throws DynException {
        if (json.isEmpty()) {
            throw new DynException("Given json is empty");
        }
        String pstmtid = this.getPreparedInsert(json);
        String stmt = preparedStatements.get(pstmtid);
        try (PreparedStatement pstmt = this.con.prepareStatement(stmt);) {
            Map<String, Integer> placeholders = preparedPlaceholders.get(pstmtid);
            Map<String, Attribute> columns = this.dyncollection.getAttributes();
            List<String> ignoredCols = new ArrayList<>();
            for (Map.Entry<String, JsonValue> curEntry : json.entrySet()) {
                String jkey = curEntry.getKey();
                // Check if table expects that data
                if (!placeholders.containsKey(jkey)) {
                    ignoredCols.add(jkey);
                    continue;
                }

                int pindex = placeholders.get(jkey);

                // Get column information
                Attribute curColumn = columns.get(jkey);
                this.setPlaceholder(pstmt, pindex, curColumn, curEntry.getValue());
            }
            if (!ignoredCols.isEmpty()) {
                String ignoredColStr = String.join(",", ignoredCols);
                String warning = "Table >" + this.table + "< does not expect data for >" + ignoredColStr + "<";
                if (!this.warnings.contains(warning)) {
                    this.warnings.add(warning);
                }
            }
            // Prevent process form accessing manual comit mode, if other process
            // is in manual comit mode (on any other location)
            commitlock.acquire();
            try {
                this.con.setAutoCommit(false);
                pstmt.executeUpdate();
                pstmt.close();
                this.con.commit();
            } catch (SQLException ex) {
                try {
                    this.con.rollback();
                } catch (SQLException ex1) {
                    Message msg = new Message("Could not rollback: " + ex1.getLocalizedMessage(),
                            MessageLevel.ERROR);
                    Logger.addDebugMessage(msg);
                }
                DynException de = new DynException(ex.getLocalizedMessage());
                de.addSuppressed(ex);
                throw de;
            } finally {
                try {
                    this.con.setAutoCommit(true);
                } catch (SQLException ex) {
                    Message msg = new Message("Could not reset autocomit mode to true!",
                            MessageLevel.ERROR);
                    Logger.addDebugMessage(msg);
                }
            }
            // Request primary key
            String idstmt = preparedStatements.get("id_" + pstmtid);
            if (idstmt != null) {
                try (PreparedStatement idpstmt = this.con.prepareStatement(idstmt)) {
                    if (idpstmt != null) {
                        try (ResultSet prs = idpstmt.executeQuery()) {
                            if (prs.next()) {
                                return prs.getObject(1);
                            }
                        }
                    }
                }
            } else {
                String warning = "Table >" + this.table + "< does not have a primary key. You should add a primary key to get id in response when creating new datasets.";
                if (!this.warnings.contains(warning)) {
                    this.warnings.add(warning);
                    Message msg = new Message(warning, MessageLevel.WARNING);
                    Logger.addDebugMessage(msg);
                }
            }
            return null;
        } catch (Exception ex) {
            String msgtxt = "Could not save dataset: Unexpected >" + ex.getClass().getSimpleName()
                    + "< exception:" + ex.getLocalizedMessage().replaceAll("[\\r\\n]", "");
            Message msg = new Message(msgtxt, MessageLevel.ERROR);
            Logger.addDebugMessage(msg);
            DynException de = new DynException(msgtxt);
            de.addSuppressed(ex);
            throw de;
        } finally {
            commitlock.release();
        }
    }

    @Override
    public String getPreparedUpdate(JsonObject json, Long id) throws DynException {
        String pstmtid = "update_" + String.join("_", json.keySet());
        this.lastStmtId = pstmtid;

        if (!this.preparedStatements.containsKey(pstmtid)) {
            this.preparedWarnings.put(pstmtid, new ArrayList<>());
            Map<String, Attribute> columns = this.dyncollection.getAttributes();
            Map<String, Integer> placeholders = new HashMap<>();

            // Build up insert statement
            StringBuilder sqlbuilder = new StringBuilder();
            sqlbuilder.append("UPDATE ");
            sqlbuilder.append("\"");
            sqlbuilder.append(this.schema);
            sqlbuilder.append("\"");
            sqlbuilder.append(".");
            sqlbuilder.append("\"");
            sqlbuilder.append(this.table);
            sqlbuilder.append("\"");
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
                if (columns.get(curKey).isIdentity()) {
                    identitycol = curKey;
                    continue;
                }
                // Get definition for current column
                Attribute attr = columns.get(curKey);

                if (foundCols > 0) {
                    sqlbuilder.append(",");
                }
                sqlbuilder.append("\"");
                sqlbuilder.append(curKey);

                // Add placeholder depending on type
                switch (attr.getType()) {
                    case "json":
                        sqlbuilder.append("\" = to_json(?::json)");
                        break;
                    case "geometry":
                        sqlbuilder.append("\" = ST_GeomFromText(?)");
                        break;
                    default:
                        sqlbuilder.append("\" = ?");
                }

                // Note placeholder
                foundCols++;
                placeholders.put(curKey, foundCols);
            }

            if (identitycol == null && id != null) {
                // Autodetect identity column
                List<Attribute> idcolumns = this.dyncollection.getIdentityAttributes();
                if (columns.isEmpty()) {
                    throw new DynException("There is no identity column in table. Could not update datasets.");
                }
                identitycol = idcolumns.get(0).getName();
            } else if (identitycol == null) {
                throw new DynException("There was no identity column given to identify the set to update.");
            }
            sqlbuilder.append(" WHERE ");
            sqlbuilder.append(identitycol);
            sqlbuilder.append(" = ?");
            foundCols++;
            placeholders.put(identitycol, foundCols);

            String sql = sqlbuilder.toString();
            Message msg = new Message("SQL: " + sql, MessageLevel.INFO);
            Logger.addDebugMessage(msg);

            this.preparedStatements.put(pstmtid, sql);
            this.preparedPlaceholders.put(pstmtid, placeholders);
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
        if (json.startsWith("[")) {
            jsonarray = jsonReader.readArray();
            jsonReader.close();
        } else if (json.contains("\"records\"")) {
            JsonObject jsonobject = jsonReader.readObject();
            jsonReader.close();
            jsonarray = jsonobject.getJsonArray("records");
        } else if (json.contains("\"list\"")) {
            JsonObject jsonobject = jsonReader.readObject();
            jsonReader.close();
            jsonarray = jsonobject.getJsonArray("list");
        } else {
            JsonObject jsonobject = jsonReader.readObject();
            jsonReader.close();
            return this.update(jsonobject, id);
        }

        Long lastid = null;
        for (int i = 0; i < jsonarray.size(); i++) {
            lastid = this.update(jsonarray.getJsonObject(i), null);
        }
        return lastid;
    }

    @Override
    public Long update(JsonObject json, Long id) throws DynException {
        String pstmtid = this.getPreparedUpdate(json, id);
        String stmt = this.preparedStatements.get(pstmtid);

        try (PreparedStatement pstmt = this.con.prepareStatement(stmt)) {
            Map<String, Integer> placeholders = this.preparedPlaceholders.get(pstmtid);
            Map<String, Attribute> columns = this.dyncollection.getAttributes();

            int usedPlaceholders = 1;
            List<String> ignoredCols = new ArrayList<>();
            for (Map.Entry<String, JsonValue> curEntry : json.entrySet()) {
                String jkey = curEntry.getKey();
                // Check if table expects that data
                if (!placeholders.containsKey(jkey)) {
                    ignoredCols.add(jkey);
                    continue;
                }

                int pindex = placeholders.get(jkey);

                // Get column information
                Attribute curColumn = columns.get(jkey);
                this.setPlaceholder(pstmt, pindex, curColumn, curEntry.getValue());
                usedPlaceholders++;
            }
            if (!ignoredCols.isEmpty()) {
                String ignoredColsStr = String.join(",", ignoredCols);
                String warning = "Table >" + this.table + "< does not expect data for >" + ignoredColsStr + "<";
                if (!this.warnings.contains(warning)) {
                    this.warnings.add(warning);
                }
            }

            // If there is a placeholder left it will be the id
            if (usedPlaceholders <= placeholders.size()) {
                try {
                    int nextId = usedPlaceholders++;
                    pstmt.setLong(nextId, id);
                } catch (SQLException ex) {
                    DynException de = new DynException("Could set id to update statement: " + ex.getLocalizedMessage().replaceAll("[\\r\\n]", ""));
                    de.addSuppressed(ex);
                    throw de;
                }
            }
            int modifieds = pstmt.executeUpdate();
            if (modifieds == 0) {
                DynException de = new DynException("Dataset with id >" + id + "< not found.");
                throw de;
            }
            return id;
        } catch (SQLException ex) {
            String msg = "Could not update dataset: " + ex.getLocalizedMessage().replaceAll("[\\r\\n]", "");
            Message msga = new Message(msg, MessageLevel.ERROR);
            Logger.addMessage(msga);
            ex.printStackTrace();
            DynException de = new DynException(msg);
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
            // Check if value is empty
            boolean isEmpty = false;
            if (value == null || value.toString().equals("null") || value.toString().isEmpty() || value.toString().equals("\"\"")) {
                isEmpty = true;
            }
            switch (col.getType()) {
                case "text":
                case "varchar":
                    if (value == null) {
                        pstmt.setNull(pindex, java.sql.Types.VARCHAR);
                    } else if (value.getValueType() == ValueType.NUMBER) {
                        pstmt.setString(pindex, value.toString());
                    } else {
                        JsonString jstr = (JsonString) value;
                        pstmt.setString(pindex, jstr.getString());
                    }
                    break;
                case "bool":
                    if (value == null || isEmpty) {
                        pstmt.setNull(pindex, java.sql.Types.BOOLEAN);
                    } else {
                        // Isn't there a better method to get the boolean value?
                        boolean bool = Boolean.parseBoolean(value.toString());
                        pstmt.setBoolean(pindex, bool);
                    }
                    break;
                case "float4":
                case "float8":
                case "numeric":
                case "decimal":
                case "real":
                case "double precision":
                    if (value == null || isEmpty) {
                        pstmt.setNull(pindex, java.sql.Types.DOUBLE);
                    } else if (value.getValueType() == ValueType.NUMBER) {
                        JsonNumber jdoub = (JsonNumber) value;
                        pstmt.setDouble(pindex, jdoub.doubleValue());
                    } else {
                        String jval = value.toString().replace("\"", "").trim();
                        pstmt.setDouble(pindex, Double.parseDouble(jval));
                    }
                    break;
                case "int2":
                case "int4":
                case "smallint":
                case "integer":
                case "bigint":
                    if (value == null || isEmpty) {
                        pstmt.setNull(pindex, java.sql.Types.INTEGER);
                    } else if (value.getValueType() == ValueType.NUMBER) {
                        JsonNumber jint = (JsonNumber) value;
                        pstmt.setInt(pindex, jint.intValue());
                    } else {
                        String jval = value.toString().replace("\"", "").trim();
                        pstmt.setInt(pindex, Integer.parseInt(jval));
                    }
                    break;
                case "int8":
                    if (value == null || isEmpty) {
                        pstmt.setNull(pindex, java.sql.Types.BIGINT);
                    } else if (value.getValueType() == ValueType.NUMBER) {
                        JsonNumber jbint = (JsonNumber) value;
                        pstmt.setLong(pindex, jbint.longValue());
                    } else {
                        String jval = value.toString().replace("\"", "").trim();
                        pstmt.setLong(pindex, Long.parseLong(jval));
                    }
                    break;
                case "timestamp":
                    if (value == null || isEmpty) {
                        pstmt.setNull(pindex, java.sql.Types.TIMESTAMP);
                    } else {
                        JsonString jts = (JsonString) value;
                        LocalDateTime ldt = DataConverter.objectToLocalDateTime(jts.getString());
                        pstmt.setTimestamp(pindex, Timestamp.valueOf(ldt));
                    }
                    break;
                case "date":
                    if (value == null || isEmpty) {
                        pstmt.setNull(pindex, java.sql.Types.DATE);
                    } else {
                        JsonString jdate = (JsonString) value;
                        LocalDate ldate = DataConverter.objectToLocalDate(jdate.getString());
                        pstmt.setDate(pindex, Date.valueOf(ldate));
                    }
                    break;
                case "json":
                    if (value == null || isEmpty) {
                        pstmt.setNull(pindex, java.sql.Types.LONGVARCHAR);
                    } else if (value.getValueType() == ValueType.OBJECT || value.getValueType() == ValueType.ARRAY) {
                        // Given value is json
                        pstmt.setString(pindex, value.toString());
                    } else {
                        // Given value is string
                        JsonString jjson = (JsonString) value;
                        pstmt.setString(pindex, jjson.getString());
                    }
                    break;
                case "geometry":
                    if (value == null || isEmpty) {
                        pstmt.setNull(pindex, java.sql.Types.OTHER);
                    } else {
                        JsonString jgeom = (JsonString) value;
                        pstmt.setObject(pindex, jgeom.getString());
                    }
                    break;
                case "point":
                    System.out.println("WARNING: Support for point type is experimental. Use geometry type instead.");
                    if (value == null || isEmpty) {
                        pstmt.setNull(pindex, java.sql.Types.OTHER);
                    } else {
                        JsonString jgeom = (JsonString) value;
                        pstmt.setObject(pindex, jgeom.getString());
                    }
//                case "line":
//                case "lseg":
//                case "box":
//                case "path":
//                case "polygon":
//                case "cirlce":
                case "bytea":
                    if (value == null || isEmpty) {
                        pstmt.setNull(pindex, java.sql.Types.BLOB);
                    } else {
                        JsonString jbytea = (JsonString) value;
                        String sbytea = jbytea.getString();

                        // Handle encoded media (from html elements like canvas)
                        byte bytes[];
                        String[] basecode = sbytea.split(";base64,", 2);
                        if (basecode.length > 1) {
                            String encodedImg = basecode[1];
                            bytes = java.util.Base64.getMimeDecoder().decode(encodedImg.getBytes(StandardCharsets.UTF_8));
                            InputStream targetStream = new ByteArrayInputStream(bytes);
                            pstmt.setBinaryStream(pindex, targetStream);
                        } else {
                            Message msg = new Message(
                                    "Write to database does not support type >" + col.getType() + "< without base64 encodeing.", MessageLevel.WARNING);
                            Logger.addDebugMessage(msg);
                            this.warnings.add("Could not save value for >" + col.getName() + "<: Please provide binary data in base64 encoded form.");
                        }
                    }
                    break;
                default:
                    Message msg = new Message(
                            "Write to database does not support type >" + col.getType() + "<", MessageLevel.ERROR);
                    Logger.addMessage(msg);
                    this.warnings.add("Could not save value for >" + col.getName() + "<: Datatype >" + col.getType() + "< is not supported.");
            }
        } catch (SQLException ex) {
            this.warnings.add("Could not save value for >" + col.getName() + "<: " + ex.getLocalizedMessage().replaceAll("[\\r\\n]", ""));
        } catch (ClassCastException ex) {
            DynException dye = new DynException("Could not interpret >"
                    + col.getName() + "< with value >" + value.toString()
                    + "< from type >" + value.getClass().getSimpleName()
                    + "< to type >" + col.getType() + "< because of ("
                    + ex.getClass().getSimpleName() + "): " + ex.getLocalizedMessage().replaceAll("[\\r\\n]", ""));
            dye.addSuppressed(ex);
            throw dye;
        }
    }

    @Override
    public Long delete(String idstr) throws DynException {
        String[] ids = idstr.split(",");
        String sql = "DELETE FROM \"" + this.schema + "\".\"" + this.table + "\" WHERE ";

        // Get name of first id column
        List<Attribute> columns = this.dyncollection.getIdentityAttributes();
        if (columns.isEmpty()) {
            throw new DynException("Could not delete from >" + this.schema + "." + this.table + " because there is no identity column.");
        }
        // Get first column
        Attribute idcol = columns.get(0);

        sql += "\"" + idcol.getName() + "\" = ";
        if (idcol.getType().equalsIgnoreCase("varchar")) {
            sql += "\"";
            sql += String.join("\" OR \"" + idcol.getName() + "\" = \"", ids);
            sql += "\"";
        } else {
            sql += String.join(" OR \"" + idcol.getName() + "\" = ", ids);
        }

        try (Statement stmt = this.con.createStatement()) {
            stmt.executeUpdate(sql);
            return null;
        } catch (SQLException ex) {
            String msg = "Could not update dataset: " + ex.getLocalizedMessage().replaceAll("[\\r\\n]", "");
            Message msga = new Message(msg, MessageLevel.ERROR);
            Logger.addMessage(msga);
            ex.printStackTrace();
            DynException de = new DynException(msg);
            de.addSuppressed(ex);
            throw de;
        }
    }

    @Override
    public List<String> getWarnings() {
        List<String> allwarns = this.warnings;
        List<String> stmtwarns = this.preparedWarnings.get(this.lastStmtId);
        if (stmtwarns != null) {
            allwarns.addAll(stmtwarns);
        }
        return allwarns;
    }
}
