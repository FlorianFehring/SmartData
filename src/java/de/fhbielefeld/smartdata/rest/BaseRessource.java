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
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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

    @POST
    @Path("createSchema")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Creates a new schema",
            description = "Creates a new schema in database")
    @APIResponse(
            responseCode = "201",
            description = "Schema was created")
    @APIResponse(
            responseCode = "304",
            description = "Schema already exists")
    @APIResponse(
            responseCode = "400",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \"No schema name given.\"]}"))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not create schema: Because of ... \"]}"))
    public Response createSchema(@Parameter(description = "Schema name", required = true,
            schema = @Schema(type = STRING, defaultValue = "public")
    ) @QueryParam("schema") String schema) {
        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        if (schema == null) {
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("No schema name given.");
            return rob.toResponse();
        }

        Connection con = this.getConnection(rob);
        if (con == null) {
            return rob.toResponse();
        }

        DynBase db = new DynBasePostgres(con);
        try {
            if (db.createSchemaIfNotExists(schema)) {
                rob.setStatus(Response.Status.CREATED);
            } else {
                rob.setStatus(Response.Status.NOT_MODIFIED);
            }
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Error retriving table names: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }

        return rob.toResponse();
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

    @DELETE
    @Path("deleteSchema")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Deletes a schema",
            description = "Deletes a schema and all its contents")
    @APIResponse(
            responseCode = "200",
            description = "Schema was deleted")
    @APIResponse(
            responseCode = "304",
            description = "Schema was not existend")
    @APIResponse(
            responseCode = "400",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \"No schema name given.\"]}"))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not delete schema: Because of ... \"]}"))
    public Response deleteSchema(@Parameter(description = "Schema name", required = true,
            schema = @Schema(type = STRING, defaultValue = "public")
    ) @QueryParam("schema") String schema) {
        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        if (schema == null) {
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("No schema name given.");
            return rob.toResponse();
        }

        Connection con = this.getConnection(rob);
        if (con == null) {
            return rob.toResponse();
        }

        DynBase db = new DynBasePostgres(con);
        try {
            if (db.deleteSchema(schema)) {
                rob.setStatus(Response.Status.OK);
            } else {
                rob.setStatus(Response.Status.NOT_MODIFIED);
            }
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Error retriving table names: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }

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
