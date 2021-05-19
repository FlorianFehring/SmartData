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
import java.util.logging.Level;

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
        try {
            Statement stmt = this.con.createStatement();
            try ( ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM information_schema.tables "
                    + "WHERE table_schema = '" + this.schema + "' "
                    + "AND table_name='" + this.name + "'")) {
                texists = rs.next();
            }
        } catch (SQLException ex) {
            DynException dex = new DynException("Could not check dataset exists: Could not get schema information: " + ex.getLocalizedMessage());
            dex.addSuppressed(ex);
            throw dex;
        }

        // If table does not exists, check if schema is present
        if (!texists) {
            DynStorage db = new DynStoragePostgres(this.con);
            if (!db.storageExists(this.schema)) {
                DynException dex = new DynException("Schema >" + this.schema + "< for table >" + this.name + "< does not exists.");
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
                String sql = "CREATE TABLE " + this.schema + "." + this.name + "(";
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
                for (Attribute curCol : table.getAttributes()) {
                    if (!curCol.isIdentity()) {
                        sql += ", \"" + curCol.getName() + "\" " + curCol.getType();
                    }
                }
                sql += ")";
                commitlock.acquire();
                this.con.setAutoCommit(true);
                Statement stmt = this.con.createStatement();
                stmt.executeUpdate(sql);
                this.con.setAutoCommit(false);
                created = true;
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
        String sql = "ALTER TABLE " + this.schema + "." + this.name;
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
            Statement stmt = this.con.createStatement();
            stmt.executeUpdate(sql);
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
    public Map<String, Attribute> getAttributes() throws DynException {
        if (!this.attributes.isEmpty()) {
            return this.attributes;
        }
        try {
            Statement stmt = this.con.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT column_name, column_default, udt_name, is_nullable, is_identity FROM information_schema.columns "
                    + "WHERE table_schema = '" + this.schema + "' "
                    + "AND table_name='" + this.name + "'");
            // Walk trough attributes
            while (rs.next()) {
                Attribute curCol = this.getColumnObject(rs);
                this.attributes.put(curCol.getName(), curCol);
            }
            rs.close();
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
        try {
            Statement stmt = this.con.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT column_name, column_default, udt_name, is_nullable, is_identity FROM information_schema.columns "
                    + "WHERE table_schema = '" + this.schema + "' "
                    + "AND table_name='" + this.name + "' "
                    + "AND column_name = '" + name + "'");
            // Walk trough attributes
            while (rs.next()) {
                column = this.getColumnObject(rs);
            }
            rs.close();
        } catch (SQLException ex) {
            DynException dex = new DynException("Could not get attribute: Could not schema information: " + ex.getLocalizedMessage());
            dex.addSuppressed(ex);
            throw dex;
        }
        // If the column was not found, check if the table is available
        if (column == null & !this.exists()) {
            DynException dex = new DynException("Table >" + this.schema + "." + this.name + "< does not exists.");
            throw dex;
        }

        return column;
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
                Statement pkstmt = this.con.createStatement();
                ResultSet pkrs = pkstmt.executeQuery(pkquery);
                if (pkrs.next()) {
                    curCol.setIsIdentity(true);
                }
                pkrs.close();
            }
            // Get if column is autoincrement
            String defaultvalue = rs.getString("column_default");
            curCol.setDefaultvalue(defaultvalue);
            if (defaultvalue != null && defaultvalue.startsWith("nextval(")) {
                curCol.setIsAutoIncrement(true);
            }

            // Get enhanced data for geometry attributes
            if (curCol.getType().equalsIgnoreCase("geometry")) {
                // Get subtype
                String geoquery = "SELECT type, srid, coord_dimension "
                        + "FROM geometry_columns WHERE f_table_schema = '"
                        + this.schema + "' AND f_table_name = '" + this.name
                        + "' AND f_geometry_column = '" + curCol.getName() + "'";
                Statement sridstmt = this.con.createStatement();
                ResultSet sridrs = sridstmt.executeQuery(geoquery);
                if (sridrs.next()) {
                    curCol.setSubtype(sridrs.getString("type"));
                    curCol.setSrid(sridrs.getInt("srid"));
                    curCol.setDimension(sridrs.getInt("coord_dimension"));
                }
                sridrs.close();
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
            try {
                Statement stmt = this.con.createStatement();
                stmt.executeQuery(
                        "SELECT UpdateGeometrySRID('" + this.schema + "', '"
                        + this.name + "','" + curCol.getName() + "'," + curCol.getSrid() + ")");
            } catch (SQLException ex) {
                DynException dex = new DynException("Could not change attributes: Could not get schema information: " + ex.getLocalizedMessage());
                dex.addSuppressed(ex);
                throw dex;
            }
        }
    }

    @Override
    public void delete() throws DynException {
        try {
            Statement stmt = this.con.createStatement();
            stmt.executeUpdate("DROP TABLE IF EXISTS " + this.name + "");
        } catch (SQLException ex) {
            DynException de = new DynException("Could not delete collection: " + ex.getLocalizedMessage());
            de.addSuppressed(ex);
            throw de;
        }
    }
}
