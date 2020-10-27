package de.fhbielefeld.smartdata.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartdata.dbo.Column;
import de.fhbielefeld.smartdata.dbo.Table;
import de.fhbielefeld.smartdata.dyntable.DynTablePostgres;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.util.List;
import javax.naming.NamingException;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PUT;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import static org.eclipse.microprofile.openapi.annotations.enums.SchemaType.STRING;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * REST Web Service
 *
 * @author Florian Fehring
 */
@Path("table")
@Tag(name = "Table", description = "Create and modify tables.")
public class TableResource {

    /**
     * Creates a new instance of RootResource
     */
    public TableResource() {
        // Init logging
        try {
            String moduleName = (String) new javax.naming.InitialContext().lookup("java:module/ModuleName");
            Logger.getInstance("SmartData", moduleName);
            Logger.setDebugMode(true);
        } catch (LoggerException | NamingException ex) {
            System.err.println("Error init logger: " + ex.getLocalizedMessage());
        }
    }

    @POST
    @Path("{table}/create")
    @Operation(summary = "Creates a table",
            description = "Creates a table based on the table definitio given.")
    @APIResponse(
            responseCode = "201",
            description = "Table created")
    @APIResponse(
            responseCode = "304",
            description = "Table allready exists")
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not create table: Because of ... \"]}"))
    public Response create(
            @Parameter(description = "Tables name", required = true, example = "mytable") @PathParam("table") String table,
            @Parameter(description = "Storage name", required = false,
                    schema = @Schema(type = STRING, defaultValue = "public"),
                    example = "myschema") @QueryParam("storage") String storage,
            Table tabledef) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try {
            // Init table access
            DynTablePostgres dtp = new DynTablePostgres(storage, tabledef.getName());
            // Get columns
            boolean created = dtp.create(tabledef);
            if (created) {
                rob.setStatus(Response.Status.CREATED);
            } else {
                rob.setStatus(Response.Status.NOT_MODIFIED);
            }
            dtp.disconnect();
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get column information: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }
        return rob.toResponse();
    }

    @GET
    @Path("{table}/getColumns")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Lists columns of a table",
            description = "Lists all columns of a table and gives base information about them.")
    @APIResponse(
            responseCode = "200",
            description = "Objects with column informations",
            content = @Content(
                    mediaType = "application/json",
                    example = "{\"list\" : [ { \"name\" : \"column1\", \"type\" : \"integer\"} ]}"
            ))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not get datasets: Because of ... \"]}"))
    public Response getColumns(
            @Parameter(description = "Tables name", required = true, example = "mytable") @PathParam("table") String table,
            @Parameter(description = "Storage name", required = false,
                    schema = @Schema(type = STRING, defaultValue = "public"),
                    example = "mystorage") @QueryParam("storage") String storage) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try {
            // Init table access
            DynTablePostgres dt = new DynTablePostgres(storage, table);
            // Get columns
            rob.add("list", dt.getColumns().values());
            dt.disconnect();
            rob.setStatus(Response.Status.OK);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get column information: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }
        return rob.toResponse();
    }

    @PUT
    @Path("{table}/addColumns")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "Adds columns to a table",
            description = "Adds columns to a table.")
    @APIResponse(
            responseCode = "200",
            description = "Number of added columns",
            content = @Content(
                    mediaType = "text/plain",
                    example = "1"
            ))
    @APIResponse(
            responseCode = "409",
            description = "Column allready exists",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Column 'columname' allready exists. \"]}"))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not get datasets: Because of ... \"]}"))
    public Response addColumns(
            @Parameter(description = "Tables name", required = true, example = "mytable") @PathParam("table") String table,
            @Parameter(description = "Storage name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            List<Column> columns) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try {
            // Init table access
            DynTablePostgres dt = new DynTablePostgres(storage, table);
            if (dt.addColumns(columns)) {
                rob.setStatus(Response.Status.CREATED);
            } else {
                rob.setStatus(Response.Status.CONFLICT);
            }
            dt.disconnect();
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get column information: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }

        return rob.toResponse();
    }

    @PUT
    @Path("{table}/changeColumn")
    @Operation(summary = "Changes the srid of a column",
            description = "Changes the srid of a column")
    @APIResponse(
            responseCode = "200",
            description = "SRID changed succsessfull")
    @APIResponse(
            responseCode = "404",
            description = "Column does not exists",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Column 'columname' does not exists. \"]}")
    )
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not change SRID: Because of ... \"]}"))
    public Response changeColumn(
            @Parameter(description = "Tables name", required = true, example = "mytable") @PathParam("table") String table,
            @Parameter(description = "Storage name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storge") String storage,
            List<Column> columns) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try {
            // Init table access
            DynTablePostgres dt = new DynTablePostgres(storage, table);
            dt.changeColumns(columns);
            dt.disconnect();
            rob.setStatus(Response.Status.OK);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not change SRID: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }

        return rob.toResponse();
    }
}
