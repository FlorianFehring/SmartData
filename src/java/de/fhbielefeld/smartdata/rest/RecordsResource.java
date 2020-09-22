package de.fhbielefeld.smartdata.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartdata.dbo.Column;
import de.fhbielefeld.smartdata.dyndata.DynDataPostgres;
import de.fhbielefeld.smartdata.dyndata.filter.EqualsFilter;
import de.fhbielefeld.smartdata.dyndata.filter.Filter;
import de.fhbielefeld.smartdata.dyndata.filter.FilterException;
import de.fhbielefeld.smartdata.dyndata.filter.FilterParser;
import de.fhbielefeld.smartdata.dyntable.DynTablePostgres;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Resource;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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

/**
 * REST Web Service for accessing the data, following the TreeQL standard with
 * some additions.
 *
 * @author Florian Fehring
 */
@Path("records")
public class RecordsResource {

    @Resource(lookup = "java:module/ModuleName")
    private String moduleName;

    @Context
    private UriInfo context;

    /**
     * Creates a new instance of RootResource
     */
    public RecordsResource() {
        // Init logging
        try {
            Logger.getInstance("SmartData", this.moduleName);
            Logger.setDebugMode(true);
        } catch (LoggerException ex) {
            System.err.println("Error init logger: " + ex.getLocalizedMessage());
        }
    }

    @POST
    @Path("{table}/")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Creates a new dataset",
            description = "Creates a new dataset stored in database")
    @APIResponse(
            responseCode = "200",
            description = "Primary key of the new created dataset.",
            content = @Content(
                    mediaType = "text/plain",
                    example = "1"
            ))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not create dataset: Because of ... \"]}"))
    public Response create(
            @Parameter(description = "Tables name", required = true, example = "mytable") @PathParam("table") String table,
            @Parameter(description = "Schema name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("schema") String schema,
            @Parameter(description = "Dataset in json format", required = true, example = "{\"value\" : 12.4}") String json) {

        if (schema == null) {
            schema = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();
        Connection con = this.getConnection(rob);
        if (con == null) {
            return rob.toResponse();
        }

        // Init data access
        DynDataPostgres dd = new DynDataPostgres(schema, table, con);

        try {
            rob.add(dd.create(json));
            for (String curWarning : dd.getWarnings()) {
                rob.addWarningMessage(curWarning);
            }
            rob.setStatus(Response.Status.OK);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not create dataset: " + ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        } finally {
            try {
                con.close();
            } catch (SQLException ex) {
                Message msg = new Message("RecordsResouce", MessageLevel.ERROR, "Could not close database connection.");
                Logger.addMessage(msg);
            }
        }
        return rob.toResponse();
    }

    @GET
    @Path("{table}/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Gets a dataset",
            description = "Delivers a dataset from database")
    @APIResponse(
            responseCode = "200",
            description = "Dataset found and delivered",
            content = @Content(
                    mediaType = "application/json",
                    example = "{\"id\" : 1, \"value\" : 12.4}"
            ))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not get dataset: Because of ... \"]}"))
    public Response get(
            @Parameter(description = "Tables name", required = true, example = "mytable") @PathParam("table") String table,
            @Parameter(description = "Dataset id", required = true, example = "1") @PathParam("id") Long id,
            @Parameter(description = "Schema name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("schema") String schema,
            @Parameter(description = "Included Columns", example = "1") @QueryParam("includes") String includes) {

        if (schema == null) {
            schema = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();
        Connection con = this.getConnection(rob);
        if (con == null) {
            return rob.toResponse();
        }

        List<Filter> filters = new ArrayList<>();
        // Init table access
        DynTablePostgres dt = new DynTablePostgres(schema, table, con);
        try {
            List<Column> idcolumns = dt.getIdentityColumns();
            if (idcolumns.isEmpty()) {
                rob.addErrorMessage("There is no identity column for table >" + table + "< could not get single dataset.");
                rob.setStatus(Response.Status.NOT_ACCEPTABLE);
                return rob.toResponse();
            } else if (idcolumns.size() > 1) {
                rob.addWarningMessage("There are more than one identity columns, try to identify on >" + idcolumns.get(0).getName() + "<");
            }

            Column idcolumn = idcolumns.get(0);
            Filter idfilter = new EqualsFilter(dt);
            idfilter.parse(idcolumn.getName() + ",eq," + id);
            filters.add(idfilter);
            // Create filter for id
        } catch (DynException ex) {
            rob.setStatus(Response.Status.NOT_ACCEPTABLE);
            rob.addErrorMessage("Could not get identity columns");
            rob.addException(ex);
            return rob.toResponse();
        } catch (FilterException ex) {
            rob.setStatus(Response.Status.NOT_ACCEPTABLE);
            rob.addErrorMessage("Could not create filter for id");
            rob.addException(ex);
            return rob.toResponse();
        }

        // Init data access
        DynDataPostgres dd = new DynDataPostgres(schema, table, con);
        try {
            String json = dd.get(includes, filters, 1, null, null, false, null, false);
            rob.add("records", json);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("Could not get data: " + ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        } finally {
            try {
                con.close();
            } catch (SQLException ex) {
                Message msg = new Message("RecordsResouce", MessageLevel.ERROR, "Could not close database connection.");
                Logger.addMessage(msg);
            }
        }
        rob.setStatus(Response.Status.OK);

        return rob.toResponse();
    }

    @GET
    @Path("{table}/")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Lists datasets from database",
            description = "Lists datasets from database that are matching the parameters.")
    @APIResponse(
            responseCode = "200",
            description = "Datasets requested",
            content = @Content(
                    mediaType = "application/json",
                    example = "{\"records\" : [{\"id\" :  1, \"value\" : 12.4}]}"
            ))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not get datasets: Because of ... \"]}"))
    public Response list(
            @Parameter(description = "Tables name", required = true, example = "mytable") @PathParam("table") String table,
            @Parameter(description = "Schema name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("schema") String schema,
            @Parameter(description = "Included Columns", example = "id,value") @QueryParam("includes") String includes,
            @Parameter(description = "Filter definition", example = "id,eq,1") @QueryParam("filter") String filter,
            @Parameter(description = "Maximum number of datasets", example = "1") @QueryParam("size") int size,
            @Parameter(description = "Page no to recive", example = "1") @QueryParam("page") String page,
            @Parameter(description = "Datasets order", example = "DESC") @QueryParam("order") String order,
            @Parameter(description = "If datasets should only counted", example = "false") @QueryParam("countonly") boolean countonly,
            @Parameter(description = "Column to get uniqe values for", example = "value") @QueryParam("unique") String unique,
            @Parameter(description = "Package values into datasets", example = "false") @QueryParam("deflatt") boolean deflatt) {

        if (schema == null) {
            schema = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();
        Connection con = this.getConnection(rob);
        if (con == null) {
            return rob.toResponse();
        }

        // Init table access
        DynTablePostgres dt = new DynTablePostgres(schema, table, con);

        List<Filter> filters = new ArrayList<>();
        if (filter != null) {
            try {
                // Build filter objects
                Filter filt = FilterParser.parse(filter, dt);
                filters.add(filt);
            } catch (FilterException ex) {
                rob.setStatus(Response.Status.BAD_REQUEST);
                rob.addErrorMessage("Could not parse filter rule >" + filter + "<: " + ex.getLocalizedMessage());
                rob.addException(ex);
                return rob.toResponse();
            }
        }

        // Init data access
        DynDataPostgres dd = new DynDataPostgres(schema, table, con);
        try {
            String json = dd.get(includes, filters, size, page, order, countonly, unique, deflatt);
            rob.add("records", json);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get data: " + ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        } finally {
            try {
                con.close();
            } catch (SQLException ex) {
                Message msg = new Message("RecordsResouce", MessageLevel.ERROR, "Could not close database connection.");
                Logger.addMessage(msg);
            }
        }
        rob.setStatus(Response.Status.OK);

        return rob.toResponse();
    }

    @PUT
    @Path("{table}/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "Updates a dataset",
            description = "Updates an existing dataset. Does nothing if the dataset does not exists.")
    @APIResponse(
            responseCode = "200",
            description = "Number of updated datasets",
            content = @Content(
                    mediaType = "text/plain",
                    example = "1"
            ))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not get datasets: Because of ... \"]}"))
    public Response update(
            @Parameter(description = "Tables name", required = true, example = "mytable") @PathParam("table") String table,
            @Parameter(description = "Datasets id", required = true, example = "1") @PathParam("id") Long id,
            @Parameter(description = "Schema name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("schema") String schema,
            @Parameter(description = "json data",
                    schema = @Schema(type = STRING, defaultValue = "public")) String json) {

        if (schema == null) {
            schema = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();
        Connection con = this.getConnection(rob);
        if (con == null) {
            return rob.toResponse();
        }

        // Init data access
        DynDataPostgres dd = new DynDataPostgres(schema, table, con);
        try {
            dd.update(json, id);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage(ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        } finally {
            try {
                con.close();
            } catch (SQLException ex) {
                Message msg = new Message("RecordsResouce", MessageLevel.ERROR, "Could not close database connection.");
                Logger.addMessage(msg);
            }
        }

        try {
            con.close();
        } catch (SQLException ex) {
            Message msg = new Message("RecordsResouce", MessageLevel.ERROR, "Could not close database connection.");
            Logger.addMessage(msg);
        }

        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }

    @PUT
    @Path("{table}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "Updates multiple datasets",
            description = "Updates existing datasets. The identity column must be included in the json.")
    @APIResponse(
            responseCode = "200",
            description = "Number of updated datasets",
            content = @Content(
                    mediaType = "text/plain",
                    example = "1"
            ))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not get datasets: Because of ... \"]}"))
    public Response update(
            @Parameter(description = "Tables name", required = true, example = "mytable") @PathParam("table") String table,
            @Parameter(description = "Schema name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("schema") String schema,
            @Parameter(description = "json data",
                    schema = @Schema(type = STRING, defaultValue = "public")) String json) {

        if (schema == null) {
            schema = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();
        Connection con = this.getConnection(rob);
        if (con == null) {
            return rob.toResponse();
        }

        // Init data access
        DynDataPostgres dd = new DynDataPostgres(schema, table, con);
        try {
            dd.update(json, null);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage(ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        } finally {
            try {
                con.close();
            } catch (SQLException ex) {
                Message msg = new Message("RecordsResouce", MessageLevel.ERROR, "Could not close database connection.");
                Logger.addMessage(msg);
            }
        }

        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }

    @DELETE
    @Path("{table}/{id}")
    @Operation(summary = "Deletes a dataset",
            description = "Deletes a dataset from database.")
    @APIResponse(
            responseCode = "200",
            description = "Number of deleted datasets",
            content = @Content(
                    mediaType = "text/plain",
                    example = "1"
            ))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not get datasets: Because of ... \"]}"))
    public Response delete(
            @Parameter(description = "Tables name", required = true, example = "mytable") @PathParam("table") String table,
            @Parameter(description = "Schema name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("schema") String schema,
            @Parameter(description = "Dataset id", required = true, example = "1") @PathParam("id") String id) {

        if (schema == null) {
            schema = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();
        Connection con = this.getConnection(rob);
        if (con == null) {
            return rob.toResponse();
        }

        // Init data access
        DynDataPostgres dd = new DynDataPostgres(schema, table, con);
        try {
            dd.delete(id);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage(ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        } finally {
            try {
                con.close();
            } catch (SQLException ex) {
                Message msg = new Message("RecordsResouce", MessageLevel.ERROR, "Could not close database connection.");
                Logger.addMessage(msg);
            }
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
            // Optain a new connection if the recived one is closed
            if (con.isClosed()) {
                Message msg = new Message("RecordsResource", MessageLevel.WARNING, "Connection was closed try to get a new one.");
                Logger.addDebugMessage(msg);
                con = ds.getConnection();
            }
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
