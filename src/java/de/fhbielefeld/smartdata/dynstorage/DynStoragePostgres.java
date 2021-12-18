package de.fhbielefeld.smartdata.dynstorage;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.smartdata.dbo.DataCollection;
import de.fhbielefeld.smartdata.dyn.DynPostgres;
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
public class DynStoragePostgres extends DynPostgres implements DynStorage {

    public DynStoragePostgres() throws DynException {
        this.connect();
    }

    public DynStoragePostgres(Connection con) {
        this.con = con;
    }

    @Override
    public Map<String, String> getAbilities() throws DynException {
        Map<String, String> abilities = new HashMap<>();
        // Check if gis is available    
        try ( Statement stmt = this.con.createStatement();  ResultSet rs = stmt.executeQuery("SELECT PostGIS_full_version() AS version")) {
            rs.next();
            String info = rs.getString("version");
            System.out.println(info);
            info = info.replaceAll("\"", "'");
            System.out.println(info);
            abilities.put("gis", "PostGIS " + info);
        } catch (SQLException ex) {
            DynException dex = new DynException(
                    "Could not get ability information: " + ex.getLocalizedMessage());
            dex.addSuppressed(ex);
            throw dex;
        }

        return abilities;
    }

    @Override
    public boolean createAbilityIfNotExists(String abilityName) throws DynException {
        boolean created = false;
        // Check if ability exists
        if (!this.getAbilities().containsKey(abilityName)) {
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
    public boolean createAbilitiesIfNotExists(Collection<String> abilityNames) throws DynException {
        boolean created = false;
        for (String abilityName : abilityNames) {
            if (this.createAbilityIfNotExists(abilityName)) {
                created = true;
            }
        }
        return created;
    }

    @Override
    public boolean storageExists(String name) throws DynException {
        String sql = "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '" + name + "'";
        try ( Statement stmtCheck = this.con.createStatement();  ResultSet rs = stmtCheck.executeQuery(sql)) {
            if (rs.next()) {
                int count = rs.getInt("count");
                if (count == 1) {
                    return true;
                }
            }
        } catch (SQLException ex) {
            Message msg = new Message("Could not check if schema >" + name + "< exists: " + ex.getLocalizedMessage(), MessageLevel.ERROR);
            msg.addException(ex);
            Logger.addMessage(msg);
        }
        return false;
    }

    @Override
    public boolean createStorageIfNotExists(Collection<String> storageNames) throws DynException {
        boolean created = false;
        for (String name : storageNames) {
            if (this.createStorageIfNotExists(name)) {
                created = true;
            }
        }
        return created;
    }

    @Override
    public boolean createStorageIfNotExists(String name) throws DynException {
        boolean created = false;
        // Check if storage exists
        boolean storageExists = this.storageExists(name);
        if (!storageExists) {
            try {
                commitlock.acquire();
                this.con.setAutoCommit(true);
                try ( Statement stmt = this.con.createStatement()) {
                    stmt.executeUpdate("CREATE SCHEMA " + name);
                }
                this.con.setAutoCommit(false);
                created = true;
            } catch (SQLException ex) {
                Message msg = new Message("Could not create schema >" + name + "<: " + ex.getLocalizedMessage(), MessageLevel.ERROR);
                msg.addException(ex);
                Logger.addMessage(msg);
            } catch (Exception ex) {
                Message msg = new Message("Could not create chema >" + name + "< an >" + ex.getClass().getSimpleName()
                        + "< occured: " + ex.getLocalizedMessage(), MessageLevel.ERROR);
                msg.addException(ex);
                Logger.addMessage(msg);
            } finally {
                try {
                    this.con.setAutoCommit(true);
                } catch (SQLException ex) {
                    Message msg = new Message("DynStoragePostgres/create",
                            MessageLevel.ERROR, "Could not reset autocomit mode to true!");
                    Logger.addDebugMessage(msg);
                }
                commitlock.release();
            }
        }
        return created;
    }

    @Override
    public Map<String, Object> getStorage(String name) throws DynException {
        Map<String, Object> information = new HashMap<>();
        information.put("name", name);
        try ( Statement stmt = this.con.createStatement();  ResultSet rs = stmt.executeQuery("SELECT * FROM information_schema.schemata WHERE schema_name = '" + name + "'")) {
            ResultSetMetaData rsmd = rs.getMetaData();
            if (rs.next()) {
                information.put("exists", true);
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    String colname = rsmd.getColumnName(i);
                    information.put(colname, rs.getObject(colname));
                }
            } else {
                information.put("exists", false);
            }
            rs.close();
        } catch (SQLException ex) {
            DynException dex = new DynException("Could not get storage: Could not get schema information: " + ex.getLocalizedMessage());
            dex.addSuppressed(ex);
            throw dex;
        }

        return information;
    }

    @Override
    public List<DataCollection> getCollections(String name) throws DynException {
        List<DataCollection> tables = new ArrayList<>();

        try ( Statement stmt = this.con.createStatement();  ResultSet rs = stmt.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + name + "'")) {
            while (rs.next()) {
                String tablename = rs.getString("table_name");
                tables.add(new DataCollection(tablename));
            }
            rs.close();
            // Check if storage exists, if there are no tables found
            if (tables.isEmpty() && !this.storageExists(name)) {
                throw new DynException("Schema >" + name + "< does not exist.");
            }
        } catch (SQLException ex) {
            DynException dex = new DynException("Could not get tables information: " + ex.getLocalizedMessage());
            dex.addSuppressed(ex);
            throw dex;
        }

        return tables;
    }

    @Override
    public boolean deleteStorage(String name) throws DynException {
        boolean deleted = false;
        // Check if storage exists
        boolean storageExists = this.storageExists(name);
        if (storageExists) {
            try {
                commitlock.acquire();
                this.con.setAutoCommit(true);
                try ( Statement stmt = this.con.createStatement()) {
                    stmt.executeUpdate("DROP SCHEMA " + name + " CASCADE");
                }
                this.con.setAutoCommit(false);
                deleted = true;
            } catch (SQLException ex) {
                Message msg = new Message("Could not delete storage >" + name + "<: " + ex.getLocalizedMessage(), MessageLevel.ERROR);
                msg.addException(ex);
                Logger.addMessage(msg);
            } catch (Exception ex) {
                Message msg = new Message("Could not delete storage >" + name + "< an >" + ex.getClass().getSimpleName()
                        + "< occured: " + ex.getLocalizedMessage(), MessageLevel.ERROR);
                msg.addException(ex);
                Logger.addMessage(msg);
            } finally {
                try {
                    this.con.setAutoCommit(true);
                } catch (SQLException ex) {
                    Message msg = new Message("DynStoragePostgres/deleteStorage",
                            MessageLevel.ERROR, "Could not reset autocomit mode to true!");
                    Logger.addDebugMessage(msg);
                }
                commitlock.release();
            }
        }
        return deleted;
    }
}
