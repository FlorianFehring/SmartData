package de.fhbielefeld.smartdata.dyncollection;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.smartdata.dbo.Attribute;
import de.fhbielefeld.smartdata.dbo.DataCollection;
import de.fhbielefeld.smartdata.dyn.DynPostgres;
import de.fhbielefeld.smartdata.dynstorage.DynStoragePostgres;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import de.fhbielefeld.smartdata.dynstorage.DynStorage;

/**
 * Class for manageing dynamic tables from postgres
 *
 * @author Florian Fehring
 */
public final class DynCollectionPostgres extends DynPostgres implements DynCollection {

    protected String schema;
    protected String name;
    protected Map<String, Attribute> attributes = new HashMap<>();

    public DynCollectionPostgres(String schema, String name) throws DynException {
        this.schema = schema;
        this.name = name;
        this.connect();
    }

    /**
     * Create access for collections with reusing existing connection
     *
     * @param schema
     * @param name
     * @param con
     * @throws DynException
     */
    public DynCollectionPostgres(String schema, String name, Connection con) throws DynException {
        this.schema = schema;
        this.name = name;
        this.con = con;
    }

    @Override
    public boolean exists() throws DynException {
        boolean texists = false;
        try ( Statement stmt = this.con.createStatement();  ResultSet rs = stmt.executeQuery(
                "SELECT * FROM information_schema.tables "
                + "WHERE table_schema = '" + this.schema + "' "
                + "AND table_name='" + this.name + "'")) {
            texists = rs.next();
        } catch (SQLException ex) {
            DynException dex = new DynException("Could not check dataset exists: Could not get schema information: " + ex.getLocalizedMessage());
            dex.addSuppressed(ex);
            throw dex;
        }

        // If table does not exists, check if schema is present
        if (!texists) {
            DynStorage db = new DynStoragePostgres(this.con);
            if (!db.storageExists(this.schema)) {
                String msg = "Schema >" + this.schema + "< for table >" + this.name + "< does not exists.";
                Message dmsg = new Message(msg, MessageLevel.ERROR);
                Logger.addDebugMessage(dmsg);
                DynException dex = new DynException(msg);
                throw dex;
            }
        }

        return texists;
    }

    @Override
    public boolean create(DataCollection table) throws DynException {
        boolean created = false;
        // Check if schema exists
        boolean schemaExists = this.exists();
        if (!schemaExists) {
            try {
                String sql = "CREATE TABLE \"" + this.schema + "\".\"" + this.name + "\"(";
                // Add identity attribute
                Attribute idcol = table.getIdentityColum();
                if (idcol != null) {
                    sql += idcol.getName();
                    if (idcol.isIsAutoIncrement()) {
                        // Create autoincrement id column
                        sql += " bigserial PRIMARY KEY";
                    } else {
                        sql += " " + idcol.getType() + " PRIMARY KEY";
                    }
                } else {
                    sql += "id bigserial PRIMARY KEY";
                }
                String foreignKeys = "";
                for (Attribute curCol : table.getAttributes()) {
                    if (!curCol.isIdentity()) {
                        sql += ", \"" + curCol.getName() + "\" " + curCol.getType();
                        if (curCol.getName().equalsIgnoreCase("id")) {
                            System.err.println("There is an column defined named id, but without beeing a identity column (set: isIdentity: true)");
                        }
                        if(!curCol.isNullable()) {
                            sql += " NOT NULL";
                        }
                    }
                    // Create forign key
                    if (curCol.getRefAttribute() != null) {
                        foreignKeys += ",";
                        foreignKeys += "CONSTRAINT " + this.name + "_" + curCol.getRefCollection() + "_" + curCol.getRefAttribute() + "_" + curCol.getName();
                        foreignKeys += " FOREIGN KEY(" + curCol.getName() + ") ";
                        foreignKeys += "REFERENCES ";
                        if (curCol.getRefStorage() != null) {
                            foreignKeys += "\"" + curCol.getRefStorage() + "\".";
                        }
                        foreignKeys += "\"" + curCol.getRefCollection() + "\"(" + curCol.getRefAttribute() + ")";
                        if (curCol.getRefOnDelete() != null) {
                            switch (curCol.getRefOnDelete()) {
                                case "CASCADE":
                                    foreignKeys += " ON DELETE CASCADE";
                                    break;
                                case "SET NULL":
                                    foreignKeys += " ON DELETE SET NULL";
                                    break;
                                case "SET DEFAULT":
                                    foreignKeys += " ON DELETE SET DEFAULT";
                                    break;
                                default:
                                    break;
                            }
                        }
                        if (curCol.getRefOnUpdate() != null && curCol.getRefOnUpdate().equals("CASCADE")) {
                            foreignKeys += " ON UPDATE CASCADE";
                        }
                    }
                }
                // Add foreign keys
                if (!foreignKeys.isEmpty()) {
                    sql += foreignKeys;
                }
                sql += ")";
                commitlock.acquire();
                this.con.setAutoCommit(true);
                try ( Statement stmt = this.con.createStatement()) {
                    stmt.executeUpdate(sql);
                    this.con.setAutoCommit(false);
                    created = true;
                }
            } catch (SQLException ex) {
                Message msg = new Message("Could not create collection >" + this.schema + "." + this.name + "<: " + ex.getLocalizedMessage(), MessageLevel.ERROR);
                msg.addException(ex);
                Logger.addMessage(msg);
            } catch (Exception ex) {
                Message msg = new Message("Could not create collection >" + this.schema
                        + "." + this.name + "< an >" + ex.getClass().getSimpleName()
                        + "< occured: " + ex.getLocalizedMessage(), MessageLevel.ERROR);
                msg.addException(ex);
                Logger.addMessage(msg);
            } finally {
                try {
                    this.con.setAutoCommit(true);
                } catch (SQLException ex) {
                    Message msg = new Message("DynDataPostgres/create",
                            MessageLevel.ERROR, "Could not reset autocomit mode to true!");
                    Logger.addDebugMessage(msg);
                }
                commitlock.release();
            }
        }
        return created;
    }

    @Override
    public boolean addAttributes(List<Attribute> columns) throws DynException {
        boolean created = false;
        String sql = "ALTER TABLE \"" + this.schema + "\".\"" + this.name + "\"";
        int i = 0;
        for (Attribute curCol : columns) {
            if (i > 0) {
                sql += ",";
            }
            sql += " ADD COLUMN \"" + curCol.getName() + "\" " + curCol.getType();
            i++;
        }
        try {
            commitlock.acquire();
            this.con.setAutoCommit(true);
            try ( Statement stmt = this.con.createStatement()) {
                stmt.executeUpdate(sql);
            }
            this.con.setAutoCommit(false);
            created = true;
        } catch (SQLException ex) {
            Message msg = new Message("Could not add columns to >" + this.schema + "." + this.name + "<: " + ex.getLocalizedMessage(), MessageLevel.ERROR);
            msg.addException(ex);
            Logger.addMessage(msg);
        } catch (Exception ex) {
            Message msg = new Message("Could not addAttributes >" + this.schema
                    + "." + this.name + "< an >" + ex.getClass().getSimpleName()
                    + "< occured: " + ex.getLocalizedMessage(), MessageLevel.ERROR);
            msg.addException(ex);
            Logger.addMessage(msg);
        } finally {
            try {
                this.con.setAutoCommit(true);
            } catch (SQLException ex) {
                Message msg = new Message("DynCollectionPostgres/addAttributes",
                        MessageLevel.ERROR, "Could not reset autocomit mode to true!");
                Logger.addDebugMessage(msg);
            }
            commitlock.release();
        }

        return created;
    }

    @Override
    public boolean delAttributes(List<Attribute> attributes) throws DynException {
        boolean deleted = false;
        String sql = "ALTER TABLE \"" + this.schema + "\".\"" + this.name + "\"";
        int i = 0;
        for (Attribute curCol : attributes) {
            if (i > 0) {
                sql += ",";
            }
            sql += " DROP COLUMN \"" + curCol.getName() + "\"";
            i++;
        }
        try {
            commitlock.acquire();
            this.con.setAutoCommit(true);
            try ( Statement stmt = this.con.createStatement()) {
                stmt.executeUpdate(sql);
            }
            this.con.setAutoCommit(false);
            deleted = true;
        } catch (SQLException ex) {
            Message msg = new Message("Could not delete columns to >" + this.schema + "." + this.name + "<: " + ex.getLocalizedMessage(), MessageLevel.ERROR);
            msg.addException(ex);
            Logger.addMessage(msg);
        } catch (Exception ex) {
            Message msg = new Message("Could not delAttributes >" + this.schema
                    + "." + this.name + "< an >" + ex.getClass().getSimpleName()
                    + "< occured: " + ex.getLocalizedMessage(), MessageLevel.ERROR);
            msg.addException(ex);
            Logger.addMessage(msg);
        } finally {
            try {
                this.con.setAutoCommit(true);
            } catch (SQLException ex) {
                Message msg = new Message("DynCollectionPostgres/delAttributes",
                        MessageLevel.ERROR, "Could not reset autocomit mode to true!");
                Logger.addDebugMessage(msg);
            }
            commitlock.release();
        }

        return deleted;
    }

    @Override
    public Map<String, Attribute> getAttributes() throws DynException {
        if (!this.attributes.isEmpty()) {
            return this.attributes;
        }
        try ( Statement stmt = this.con.createStatement();  ResultSet rs = stmt.executeQuery(
                "SELECT column_name, column_default, udt_name, is_nullable, is_identity FROM information_schema.columns "
                + "WHERE table_schema = '" + this.schema + "' "
                + "AND table_name='" + this.name + "'")) {
            // Walk trough attributes
            while (rs.next()) {
                Attribute curCol = this.getColumnObject(rs);
                this.attributes.put(curCol.getName(), curCol);
            }
            // Check if table does not exists when there is no column
            if (this.attributes.isEmpty()) {
                if (!this.exists()) {
                    throw new DynException("Table >" + this.schema + "." + this.name + "< does not exists.");
                }
            }
        } catch (SQLException ex) {
            DynException dex = new DynException("Could not get attributes: Could not get schema information: " + ex.getLocalizedMessage());
            dex.addSuppressed(ex);
            throw dex;
        }

        return this.attributes;
    }

    @Override
    public Attribute getAttribute(String name) throws DynException {
        // Use from prev call if possible
        if (this.attributes.containsKey(name)) {
            return this.attributes.get(name);
        }

        Attribute column = null;
        try ( Statement stmt = this.con.createStatement();  ResultSet rs = stmt.executeQuery(
                "SELECT column_name, column_default, udt_name, is_nullable, is_identity FROM information_schema.columns "
                + "WHERE table_schema = '" + this.schema + "' "
                + "AND table_name='" + this.name + "' "
                + "AND column_name = '" + name + "'");) {
            // Walk trough attributes
            while (rs.next()) {
                column = this.getColumnObject(rs);
            }
        } catch (SQLException ex) {
            DynException dex = new DynException("Could not get attribute: Could not schema information: " + ex.getLocalizedMessage());
            dex.addSuppressed(ex);
            throw dex;
        }
        // If the column was not found, check if the table is available
        if (column == null & !this.exists()) {
            String msg = "Table >" + this.schema + "." + this.name + "< does not exists.";
            Message dmsg = new Message(msg, MessageLevel.ERROR);
            Logger.addDebugMessage(dmsg);
            DynException dex = new DynException(msg);
            throw dex;
        }

        return column;
    }

    @Override
    public Attribute getReferenceTo(String collection) throws DynException {
        Attribute ref = null;
        for (Attribute curAttr : this.getAttributes().values()) {
            String col = curAttr.getRefCollection();
            if (col != null && col.equals(collection)) {
                ref = curAttr;
                break;
            }
        }
        return ref;
    }

    /**
     * Creates a column object from an result set
     *
     * @param rs ResultSet containing column informations
     * @return Created Attribute object
     * @throws DynException
     */
    private Attribute getColumnObject(ResultSet rs) throws DynException {
        Attribute curCol = null;
        try {
            curCol = new Attribute(rs.getString("column_name"), rs.getString("udt_name"));
            curCol.setIsNullable(rs.getBoolean("is_nullable"));
            // is_identity is only set when SQL standard "GENERATED BY DEFAULT AS IDENTITY" is used
            curCol.setIsIdentity(rs.getBoolean("is_identity"));
            // Check if belongs to primary key
            if (!curCol.isIdentity()) {
                String pkquery = "SELECT  k.column_name "
                        + "FROM information_schema.table_constraints AS c "
                        + "JOIN information_schema.key_column_usage AS k "
                        + "ON c.table_name = k.table_name "
                        + "AND c.constraint_catalog = k.constraint_catalog "
                        + "AND c.constraint_schema = k.constraint_schema "
                        + "AND c.constraint_name = k.constraint_name "
                        + "WHERE c.constraint_type = 'PRIMARY KEY' "
                        + "AND k.table_schema = '" + this.schema + "' "
                        + "AND k.table_name = '" + this.name + "' "
                        + "AND k.column_name = '" + curCol.getName() + "';";
                try ( Statement pkstmt = this.con.createStatement();  ResultSet pkrs = pkstmt.executeQuery(pkquery);) {
                    if (pkrs.next()) {
                        curCol.setIsIdentity(true);
                    }
                }
            }
            // Get if column is autoincrement
            String defaultvalue = rs.getString("column_default");
            curCol.setDefaultvalue(defaultvalue);
            if (defaultvalue != null && defaultvalue.startsWith("nextval(")) {
                curCol.setIsAutoIncrement(true);
            }
            // Get if column is a reference
            String rquery = "SELECT "
                    + "tc.constraint_name, "
                    + "tc.table_name, "
                    + "kcu.column_name, "
                    + "ccu.table_schema AS foreign_table_schema, "
                    + "ccu.table_name AS foreign_table_name, "
                    + "ccu.column_name AS foreign_column_name "
                    + "FROM "
                    + "information_schema.table_constraints AS tc "
                    + "JOIN information_schema.key_column_usage AS kcu "
                    + "ON tc.constraint_name = kcu.constraint_name "
                    + "AND tc.table_schema = kcu.table_schema "
                    + "JOIN information_schema.constraint_column_usage AS ccu "
                    + "ON ccu.constraint_name = tc.constraint_name "
                    + "AND ccu.table_schema = tc.table_schema "
                    + "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema='"
                    + this.schema + "' AND tc.table_name='" + this.name
                    + "' and kcu.column_name='" + curCol.getName() + "';";
            try ( Statement rstmt = this.con.createStatement();  ResultSet rrs = rstmt.executeQuery(rquery)) {
                if (rrs.next()) {
                    curCol.setRefName(rrs.getString("constraint_name"));
                    curCol.setRefCollection(rrs.getString("foreign_table_name"));
                    curCol.setRefStorage(rrs.getString("foreign_table_schema"));
                    curCol.setRefAttribute(rrs.getString("foreign_column_name"));
                }
            }
            // Get enhanced information about reference
            if (curCol.getRefName() != null) {
                String rtquery = "SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint WHERE conname = '" + curCol.getRefName() + "'";
                try ( Statement rstmt = this.con.createStatement();  ResultSet rrs = rstmt.executeQuery(rtquery)) {
                    if (rrs.next()) {
                        String def = rrs.getString("def");
                        // Search onUpdate
                        if (def.contains("ON UPDATE CASCADE")) {
                            curCol.setRefOnUpdate("CASCADE");
                        }
                        // Search onDelete
                        if (def.contains("ON DELETE CASCADE")) {
                            curCol.setRefOnDelete("CASCADE");
                        }
                        if (def.contains("ON DELETE SET NULL")) {
                            curCol.setRefOnDelete("SET NULL");
                        }
                        if (def.contains("ON DELETE SET DEFAULT")) {
                            curCol.setRefOnDelete("SET DEFAULT");
                        }
                    }
                }
            }

            // Get enhanced data for geometry attributes
            if (curCol.getType().equalsIgnoreCase("geometry")) {
                // Get subtype
                String geoquery = "SELECT type, srid, coord_dimension "
                        + "FROM geometry_columns WHERE f_table_schema = '"
                        + this.schema + "' AND f_table_name = '" + this.name
                        + "' AND f_geometry_column = '" + curCol.getName() + "'";
                try ( Statement sridstmt = this.con.createStatement();  ResultSet sridrs = sridstmt.executeQuery(geoquery)) {
                    if (sridrs.next()) {
                        curCol.setSubtype(sridrs.getString("type"));
                        curCol.setSrid(sridrs.getInt("srid"));
                        curCol.setDimension(sridrs.getInt("coord_dimension"));
                    }
                }
            }
        } catch (SQLException ex) {
            DynException dex = new DynException("Could not get coulmn object: Could not get schema information: " + ex.getLocalizedMessage());
            dex.addSuppressed(ex);
            throw dex;
        }
        return curCol;
    }

    @Override
    public List<Attribute> getIdentityAttributes() throws DynException {
        List<Attribute> idattrs = new ArrayList<>();
        for (Attribute curAttr : this.getAttributes().values()) {
            if (curAttr.isIdentity()) {
                idattrs.add(curAttr);
            }
        }
        return idattrs;
    }

    @Override
    public List<Attribute> getGeoAttributes() throws DynException {
        List<Attribute> geoattrs = new ArrayList<>();
        for (Attribute curAttr : this.getAttributes().values()) {
            if (curAttr.getType().equalsIgnoreCase("geometry")) {
                geoattrs.add(curAttr);
            }
        }
        return geoattrs;
    }

    @Override
    public void changeAttributeName(String oldname, String newname) throws DynException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeAttributes(List<Attribute> columns) throws DynException {
        for (Attribute curCol : columns) {
            try ( Statement stmt = this.con.createStatement()) {
                // Update type
                if (curCol.getType() != null) {
                    stmt.executeUpdate("ALTER TABLE \"" + this.schema + "\".\"" + this.name
                            + "\" ALTER COLUMN \"" + curCol.getName() + "\" TYPE " + curCol.getType());
                }
                // Update SRID
                if (curCol.getSrid() != null) {
                    stmt.executeQuery(
                            "SELECT UpdateGeometrySRID('" + this.schema + "', '"
                            + this.name + "','" + curCol.getName() + "'," + curCol.getSrid() + ")");
                }
            } catch (SQLException ex) {
                DynException dex = new DynException("Could not change attributes: Could not get schema information: " + ex.getLocalizedMessage());
                dex.addSuppressed(ex);
                throw dex;
            }
        }
    }

    @Override
    public void delete() throws DynException {
        try ( Statement stmt = this.con.createStatement()) {
            stmt.executeUpdate("DROP TABLE IF EXISTS \"" + this.schema + "\".\"" + this.name + "\"");
        } catch (SQLException ex) {
            DynException de = new DynException("Could not delete collection: " + ex.getLocalizedMessage());
            de.addSuppressed(ex);
            throw de;
        }
    }
}
