package de.fhbielefeld.smartdata.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartdata.dynbase.DynBase;
import de.fhbielefeld.smartdata.dynbase.DynBasePostgres;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.sql.Connection;
import java.sql.SQLException;
import javax.annotation.Resource;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import static org.eclipse.microprofile.openapi.annotations.enums.SchemaType.STRING;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/**
 * Ressource for accessing database informations
 *
 * @author Florian Fehring
 */
@Path("base")
public class BaseRessource {

    @Resource(lookup = "java:module/ModuleName")
    private String moduleName;

    /**
     * Creates a new instance of RootResource
     */
    public BaseRessource() {
        // Init logging
        try {
            Logger.getInstance("SmartData", this.moduleName);
            Logger.setDebugMode(true);
        } catch (LoggerException ex) {
            System.err.println("Error init logger: " + ex.getLocalizedMessage());
        }
    }

    @GET
    @Path("getTables")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Lists all tables",
            description = "Lists all tables of a schema")
    @APIResponse(
            responseCode = "200",
            description = "List with table informations",
            content = @Content(
                    mediaType = "application/json",
                    example = "{ \"list\" : { \"name\" : \"table1\"}}"
            ))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not get datasets: Because of ... \"]}"))
    public Response getTables(
            @Parameter(description = "Schema name", required = false,
                    schema = @Schema(type = STRING, defaultValue = "public")
            ) @QueryParam("schema") String schema) {

        if (schema == null) {
            schema = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();
        Connection con = this.getConnection(rob);
        if (con == null) {
            return rob.toResponse();
        }

        DynBase db = new DynBasePostgres(con);
        try {
            rob.add("list", db.getTables(schema));
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Error retriving table names: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }

        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }

    /**
     * Gets the db connection
     *
     * @param rob ResponseObjectBuilder to add errormessages
     * @return Connection or null if could not get
     */
    private Connection getConnection(ResponseObjectBuilder rob) {
        Connection con = null;
        try {
            InitialContext ctx = new InitialContext();
            DataSource ds = (DataSource) ctx.lookup("jdbc/smartdata");
            con = ds.getConnection();
        } catch (NamingException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not access connection pool: " + ex.getLocalizedMessage());
            rob.addException(ex);
        } catch (SQLException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not conntect to database: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }
        return con;
    }
}
