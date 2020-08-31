package de.fhbielefeld.smartdata.dynbase;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.smartdata.dbo.Table;
import de.fhbielefeld.smartdata.dynbase.DynBase;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Methods for getting informations and createing global structures to a
 * postgres database.
 *
 * @author Florian Fehring
 */
public class DynBasePostgres extends DynBase {

    private Connection con;

    public DynBasePostgres(Connection con) {
        this.con = con;
    }

    @Override
    public Collection<String> getAbilities() throws DynException {
        List<String> abilities = new ArrayList<>();
        abilities.add("gis");
        return abilities;
    }

    @Override
    public Map<String, Object> getAbility(String abilityName) throws DynException {
        Map<String, Object> information = new HashMap<>();
        if (abilityName.equalsIgnoreCase("gis")) {
            try {
                Statement stmt = this.con.createStatement();
                ResultSet rs = stmt.executeQuery("select * from pg_proc where proname = 'postgis_full_version'");
                if (!rs.next()) {
                    return information;
                }
            } catch (SQLException ex) {
                DynException dex = new DynException(
                        "Could not get ability information: " + ex.getLocalizedMessage());
                dex.addSuppressed(ex);
                throw dex;
            }
            try {
                Statement stmt = this.con.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT PostGIS_full_version() AS version");
                rs.next();
                information.put("exists", true);
                information.put("vendor", "postgis");
                information.put("version", rs.getString("version"));
            } catch (SQLException ex) {
                DynException dex = new DynException(
                        "Could not get ability information: " + ex.getLocalizedMessage());
                dex.addSuppressed(ex);
                throw dex;
            }
        } else {
            throw new DynException("Ability >" + abilityName + "< is not supported by "
                    + this.getClass().getSimpleName());
        }
        return information;
    }

    @Override
    public boolean createAbilityIfNotExists(String abilityName) throws DynException {
        boolean created = false;
        // Check if ability exists
        Map<String, Object> abilityInfo = this.getAbility(abilityName);
        if (!abilityInfo.containsKey("exists")) {
            if (abilityName.equalsIgnoreCase("gis")) {
                throw new DynException("You must install postgis support as "
                        + "superuser useing the following commands:\n\r"
                        + "CREATE EXTENSION postgis;\n\r"
                        + "CREATE EXTENSION postgis_sfcgal;");
                //Following does not work because executor must be SUPER USER
//            try {
                // Enable PostGIS (as of 3.0 contains just geometry/geography)
//                Statement stmt = con.createStatement();
//                stmt.executeUpdate("CREATE EXTENSION postgis");
                // Enable PostGIS Advanced 3D and other geoprocessing algorithms
                // Note: sfcgal not available with all distributions
//                Statement stmt2 = con.createStatement();
//                stmt2.executeUpdate("CREATE EXTENSION postgis_sfcgal");
//            } catch (SQLException ex) {
//                DynBaseException dex = new DynBaseException("Could not create ability >postgis<");
//                dex.addSuppressed(ex);
//                throw dex;
//            }
            } else {
                throw new DynException("Ability >" + abilityName + "< is not supported by "
                        + this.getClass().getSimpleName());
            }
        }
        return created;
    }

    @Override
    public boolean schemaExists(String schemaName) throws DynException {
        try {
            Statement stmtCheck = this.con.createStatement();
            String sql = "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '" + schemaName + "'";
            ResultSet rs = stmtCheck.executeQuery(sql);
            if (rs.next()) {
                int count = rs.getInt("count");
                if (count == 1) {
                    return true;
                }
            }
        } catch (SQLException ex) {
            Message msg = new Message("Could not check if schema >" + schemaName + "< exists: " + ex.getLocalizedMessage(), MessageLevel.ERROR);
            msg.addException(ex);
            Logger.addMessage(msg);
        }
        return false;
    }

    @Override
    public boolean createSchemaIfNotExists(String schemaName) throws DynException {
        boolean created = false;
        // Check if schema exists
        boolean schemaExists = this.schemaExists(schemaName);
        if (!schemaExists) {
            try {
                this.con.setAutoCommit(true);
                Statement stmt = this.con.createStatement();
                stmt.executeUpdate("CREATE SCHEMA " + schemaName);
                this.con.setAutoCommit(false);
                created = true;
            } catch (SQLException ex) {
                Message msg = new Message("Could not create schema >" + schemaName + "<: " + ex.getLocalizedMessage(), MessageLevel.ERROR);
                msg.addException(ex);
                Logger.addMessage(msg);
            }
        }
        return created;
    }

    @Override
    public Map<String, Object> getSchema(String schemaName) throws DynException {
        Map<String, Object> information = new HashMap<>();
        information.put("name", schemaName);
        try {
            Statement stmt = this.con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM information_schema.schemata WHERE schema_name = '" + schemaName + "'");
            ResultSetMetaData rsmd = rs.getMetaData();
            if (rs.next()) {
                information.put("exists", true);
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    String name = rsmd.getColumnName(i);
                    information.put(name, rs.getObject(name));
                }
            } else {
                information.put("exists", false);
            }
        } catch (SQLException ex) {
            DynException dex = new DynException("Could not get schema information");
            dex.addSuppressed(ex);
            throw dex;
        }

        return information;
    }

    @Override
    public List<Table> getTables(String schemaName) throws DynException {
        List<Table> tables = new ArrayList<>();

        try {
            Statement stmt = this.con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schemaName + "'");
            while (rs.next()) {
                String tablename = rs.getString("table_name");
                tables.add(new Table(tablename));
            }
            // Check if schema exists, if there are no tables found
            if (tables.isEmpty() && !this.schemaExists(schemaName)) {
                throw new DynException("Schema >" + schemaName + "< does not exist.");
            }
        } catch (SQLException ex) {
            DynException dex = new DynException("Could not get tables information");
            dex.addSuppressed(ex);
            throw dex;
        }

        return tables;
    }

}
