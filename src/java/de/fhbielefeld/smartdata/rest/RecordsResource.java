package de.fhbielefeld.smartdata.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartdata.dbo.Column;
import de.fhbielefeld.smartdata.dyndata.DynDataPostgres;
import de.fhbielefeld.smartdata.dyndata.filter.EqualsFilter;
import de.fhbielefeld.smartdata.dyndata.filter.Filter;
import de.fhbielefeld.smartdata.dyndata.filter.FilterException;
import de.fhbielefeld.smartdata.dyndata.filter.FilterParser;
import de.fhbielefeld.smartdata.dyntable.DynCollectionPostgres;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.util.ArrayList;
import java.util.List;
import javax.naming.NamingException;
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
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * REST Web Service for accessing the data, following the TreeQL standard with
 * some additions.
 *
 * @author Florian Fehring
 */
@Path("records")
@Tag(name = "Records", description = "Accessing, inserting, updateing and deleting datasets.")
public class RecordsResource {

    /**
     * Creates a new instance of RootResource
     */
    public RecordsResource() {
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
    @Path("{collection}/")
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
            @Parameter(description = "Collections name", required = true, example = "mycollection") @PathParam("collection") String collection,
            @Parameter(description = "Storage name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            @Parameter(description = "Dataset in json format", required = true, example = "{\"value\" : 12.4}") String json) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try {
            // Init data access
            DynDataPostgres dd = new DynDataPostgres(storage, collection);
            rob.add(dd.create(json));
            for (String curWarning : dd.getWarnings()) {
                rob.addWarningMessage(curWarning);
            }
            dd.disconnect();
            rob.setStatus(Response.Status.OK);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not create dataset: " + ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        }
        return rob.toResponse();
    }

    @GET
    @Path("{collection}/{id}")
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
            @Parameter(description = "Collections name", required = true, example = "mycollection") @PathParam("collection") String collection,
            @Parameter(description = "Dataset id", required = true, example = "1") @PathParam("id") Long id,
            @Parameter(description = "Storage name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            @Parameter(description = "Included Columns", example = "1") @QueryParam("includes") String includes) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        List<Filter> filters = new ArrayList<>();
        // Init collection access
        try {
            DynCollectionPostgres dt = new DynCollectionPostgres(storage, collection);
            List<Column> idcolumns = dt.getIdentityColumns();
            if (idcolumns.isEmpty()) {
                rob.addErrorMessage("There is no identity column for collection >" + collection + "< could not get single dataset.");
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

        try {
            // Init data access
            DynDataPostgres dd = new DynDataPostgres(storage, collection);
            String json = dd.get(includes, filters, 1, null, null, false, null, false);
            rob.add("records", json);
            dd.disconnect();
        } catch (DynException ex) {
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("Could not get data: " + ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        }
        rob.setStatus(Response.Status.OK);

        return rob.toResponse();
    }

    @GET
    @Path("{collection}/")
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
            @Parameter(description = "Collections name", required = true, example = "mycollection") @PathParam("collection") String collection,
            @Parameter(description = "Storage name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            @Parameter(description = "Included Columns", example = "id,value") @QueryParam("includes") String includes,
            @Parameter(description = "Filter definition", example = "id,eq,1") @QueryParam("filter") String filter,
            @Parameter(description = "Maximum number of datasets", example = "1") @QueryParam("size") int size,
            @Parameter(description = "Page no to recive", example = "1") @QueryParam("page") String page,
            @Parameter(description = "Datasets order", example = "DESC") @QueryParam("order") String order,
            @Parameter(description = "If datasets should only counted", example = "false") @QueryParam("countonly") boolean countonly,
            @Parameter(description = "Column to get uniqe values for", example = "value") @QueryParam("unique") String unique,
            @Parameter(description = "Package values into datasets", example = "false") @QueryParam("deflatt") boolean deflatt) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        List<Filter> filters = new ArrayList<>();
        if (filter != null) {
            try {
                // Init collection access
                DynCollectionPostgres dt = new DynCollectionPostgres(storage, collection);
                // Build filter objects
                Filter filt = FilterParser.parse(filter, dt);
                filters.add(filt);
            } catch (FilterException ex) {
                rob.setStatus(Response.Status.BAD_REQUEST);
                rob.addErrorMessage("Could not parse filter rule >" + filter + "<: " + ex.getLocalizedMessage());
                rob.addException(ex);
                return rob.toResponse();
            } catch (DynException ex) {
                rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
                rob.addErrorMessage("Could not get data: " + ex.getLocalizedMessage());
                rob.addException(ex);
                return rob.toResponse();
            }
        }

        try {
            // Init data access
            DynDataPostgres dd = new DynDataPostgres(storage, collection);
            String json = dd.get(includes, filters, size, page, order, countonly, unique, deflatt);
            rob.add("records", json);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get data: " + ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        }
        rob.setStatus(Response.Status.OK);

        return rob.toResponse();
    }

    @PUT
    @Path("{collection}/{id}")
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
            @Parameter(description = "Collections name", required = true, example = "mycollection") @PathParam("collection") String collection,
            @Parameter(description = "Datasets id", required = true, example = "1") @PathParam("id") Long id,
            @Parameter(description = "Storage name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            @Parameter(description = "json data",
                    schema = @Schema(type = STRING, defaultValue = "public")) String json) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try {
            // Init data access
            DynDataPostgres dd = new DynDataPostgres(storage, collection);
            dd.update(json, id);
            dd.disconnect();
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage(ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        }

        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }

    @PUT
    @Path("{collection}")
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
            @Parameter(description = "Collections name", required = true, example = "mycollection") @PathParam("collection") String collection,
            @Parameter(description = "Storage name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            @Parameter(description = "json data",
                    schema = @Schema(type = STRING, defaultValue = "public")) String json) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try {
            // Init data access
            DynDataPostgres dd = new DynDataPostgres(storage, collection);
            dd.update(json, null);
            dd.disconnect();
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage(ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        }

        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }

    @DELETE
    @Path("{collection}/{id}")
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
            @Parameter(description = "Collections name", required = true, example = "mycollection") @PathParam("collection") String collection,
            @Parameter(description = "Storage name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            @Parameter(description = "Dataset id", required = true, example = "1") @PathParam("id") String id) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try {
            // Init data access
            DynDataPostgres dd = new DynDataPostgres(storage, collection);
            dd.delete(id);
            dd.disconnect();
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage(ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        }

        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }
}
