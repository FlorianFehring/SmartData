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
    protected List<String> warnings = new ArrayList<>();
    
    @Override
    public void connect() throws DynException {
        Configuration conf = new Configuration();
        String jndi = conf.getProperty("postgres.jndi");
        if(jndi == null)
            jndi = "jdbc/SmartData";
        
        try {
            InitialContext ctx = new InitialContext();
            DataSource ds = (DataSource) ctx.lookup(jndi);
            this.con = ds.getConnection();
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

    @Override
    public void disconnect() throws DynException {
        try {
            con.close();
        } catch (SQLException ex) {
            Message msg = new Message("RecordsResouce", MessageLevel.ERROR, "Could not close database connection.");
            Logger.addMessage(msg);
            throw new DynException("Could not close database connection");
        }
    }
    
    @Override
    public List<String> getWarnings() {
        return warnings;
    }
}
