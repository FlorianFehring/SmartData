package de.fhbielefeld.smartdata.dyn;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.smartdata.config.Configuration;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

/**
 * Postgres basic functions
 *
 * @author Florian Fehring
 */
public class DynPostgres implements Dyn {

    protected Connection con;
    protected static Semaphore commitlock;
    protected List<String> warnings = new ArrayList<>();

    @Override
    public void connect() throws DynException {
        try {
            // Only create new connection if there is no active one
            if (this.con == null || this.con.isClosed()) {
                Configuration conf = new Configuration();
                String jndi = conf.getProperty("postgres.jndi");
                if (jndi == null) {
                    jndi = "jdbc/SmartData";
                }

                try {
                    InitialContext ctx = new InitialContext();
                    DataSource ds = (DataSource) ctx.lookup(jndi);
                    this.con = ds.getConnection();
                    // Create semaphore
                    commitlock = new Semaphore(1);
                } catch (NamingException ex) {
                    Message msg = new Message("", MessageLevel.ERROR, "Could not access connection pool: " + ex.getLocalizedMessage());
                    Logger.addMessage(msg);
                    DynException dex = new DynException("Could not access connection pool: " + ex.getLocalizedMessage());
                    dex.addSuppressed(ex);
                    throw dex;
                } catch (SQLException ex) {
                    Message msg = new Message("", MessageLevel.ERROR, "Could not conntect to database: " + ex.getLocalizedMessage());
                    Logger.addMessage(msg);
                    DynException dex = new DynException("Could not conntect to database: " + ex.getLocalizedMessage());
                    dex.addSuppressed(ex);
                    throw dex;
                }
            }
        } catch (SQLException ex) {
            Message msg = new Message("", MessageLevel.ERROR, "Could not check connection: " + ex.getLocalizedMessage());
            Logger.addMessage(msg);
            DynException dex = new DynException("Could not check connection: " + ex.getLocalizedMessage());
            dex.addSuppressed(ex);
            throw dex;
        }
    }
    
    @Override
    public void close() throws DynException {
        try {
            if(!this.con.isClosed())
                this.con.close();
        } catch (SQLException ex) {
            Message msg = new Message("", MessageLevel.ERROR, "Could not close connection: " + ex.getLocalizedMessage());
            Logger.addMessage(msg);
            DynException dex = new DynException("Could not close connection: " + ex.getLocalizedMessage());
            dex.addSuppressed(ex);
            throw dex;
        }
    }

    @Override
    public List<String> getWarnings() {
        return warnings;
    }
}
