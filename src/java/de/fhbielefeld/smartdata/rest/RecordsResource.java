package de.fhbielefeld.smartdata.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartdata.config.Configuration;
import de.fhbielefeld.smartdata.dbo.Attribute;
import de.fhbielefeld.smartdata.dyncollection.DynCollection;
import de.fhbielefeld.smartdata.dyncollection.DynCollectionMongo;
import de.fhbielefeld.smartdata.dynrecords.DynRecordsPostgres;
import de.fhbielefeld.smartdata.dynrecords.filter.EqualsFilter;
import de.fhbielefeld.smartdata.dynrecords.filter.Filter;
import de.fhbielefeld.smartdata.dynrecords.filter.FilterException;
import de.fhbielefeld.smartdata.dynrecords.filter.FilterParser;
import de.fhbielefeld.smartdata.dyncollection.DynCollectionPostgres;
import de.fhbielefeld.smartdata.dynrecords.DynRecords;
import de.fhbielefeld.smartdata.dynrecords.DynRecordsMongo;
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

        // Init access
        DynRecords dynr;
        Configuration conf = new Configuration();
        try {
            if (conf.getProperty("mongo.url") != null) {
                dynr = new DynRecordsMongo();
            } else {
                dynr = new DynRecordsPostgres(storage, collection);
            }
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get data: " + ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        }

        try {
            rob.add(dynr.create(json));
            for (String curWarning : dynr.getWarnings()) {
                rob.addWarningMessage(curWarning);
            }
            rob.setStatus(Response.Status.OK);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not create dataset: " + ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        } finally {
            dynr.disconnect();
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
            @Parameter(description = "Name of the collection to get data from (Tablename, Documentspace)", required = true, example = "mycollection") @PathParam("collection") String collection,
            @Parameter(description = "Datasets id", required = true, example = "1") @PathParam("id") Long id,
            @Parameter(description = "Name of the storage to look at (public, smartdata_xyz, ...)",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            @Parameter(description = "Attributes to include, comata separated", example = "1") @QueryParam("includes") String includes) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        // Init access
        DynCollection dync;
        DynRecords dynr;
        Configuration conf = new Configuration();
        try {
            if (conf.getProperty("mongo.url") != null) {
                dync = new DynCollectionMongo(storage, collection);
                dynr = new DynRecordsMongo();
            } else {
                dync = new DynCollectionPostgres(storage, collection);
                dynr = new DynRecordsPostgres(storage, collection);
            }
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get data: " + ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        }

        List<Filter> filters = new ArrayList<>();
        // Init collection access
        try {
            List<Attribute> idattrs = dync.getIdentityAttributes();
            if (idattrs.isEmpty()) {
                rob.addErrorMessage("There is no identity attribute for collection >" + collection + "< could not get single dataset.");
                rob.setStatus(Response.Status.NOT_ACCEPTABLE);
                return rob.toResponse();
            } else if (idattrs.size() > 1) {
                rob.addWarningMessage("There are more than one identity attributes, try to identify on >" + idattrs.get(0).getName() + "<");
            }

            Attribute idattr = idattrs.get(0);
            Filter idfilter = new EqualsFilter(dync);
            idfilter.parse(idattr.getName() + ",eq," + id);
            filters.add(idfilter);
            // Create filter for id
        } catch (DynException ex) {
            dynr.disconnect();
            rob.setStatus(Response.Status.NOT_ACCEPTABLE);
            rob.addErrorMessage("Could not get identity attributes");
            rob.addException(ex);
            return rob.toResponse();
        } catch (FilterException ex) {
            dynr.disconnect();
            rob.setStatus(Response.Status.NOT_ACCEPTABLE);
            rob.addErrorMessage("Could not create filter for id");
            rob.addException(ex);
            return rob.toResponse();
        } finally {
            dync.disconnect();
        }

        try {
            String json = dynr.get(includes, filters, 1, null, null, false, null, false);
            rob.add("records", json);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("Could not get data: " + ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        } finally {
            dynr.disconnect();
        }
        rob.setStatus(Response.Status.OK);

        return rob.toResponseStream();
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
            @Parameter(description = "Name of the collection to get data from (Tablename, Documentspace)", required = true, example = "mycollection") @PathParam("collection") String collection,
            @Parameter(description = "Name of the storage to look at (public, smartdata_xyz, ...)",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            @Parameter(description = "Attributes to include, comata separated", example = "id,value") @QueryParam("includes") String includes,
            @Parameter(description = "Definition of an filter (e.g. where id equals 1)", example = "id,eq,1") @QueryParam("filter") String filter,
            @Parameter(description = "Maximum number of datasets", example = "1") @QueryParam("size") int size,
            @Parameter(description = "Page no to recive", example = "1") @QueryParam("page") String page,
            @Parameter(description = "Datasets order column and order kind", example = "column[,desc]") @QueryParam("order") String order,
            @Parameter(description = "If datasets should only be counted (untested)", example = "false") @QueryParam("countonly") boolean countonly,
            @Parameter(description = "Attribute to get uniqe values for (untested)", example = "value") @QueryParam("unique") String unique,
            @Parameter(description = "Package values into datasets (untested)", example = "false") @QueryParam("deflatt") boolean deflatt) {

        if (storage == null) {
            storage = "public";
        }

        // Catch negative limits
        if (size < 0) {
            size = 0;
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        // Init access
        DynCollection dync;
        DynRecords dynr;
        Configuration conf = new Configuration();
        try {
            if (conf.getProperty("mongo.url") != null) {
                dync = new DynCollectionMongo(storage, collection);
                dynr = new DynRecordsMongo();
            } else {
                dync = new DynCollectionPostgres(storage, collection);
                dynr = new DynRecordsPostgres(storage, collection);
            }
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get data: " + ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        }

        List<Filter> filters = new ArrayList<>();

        try {
            if (filter != null) {
                // Build filter objects
                Filter filt = FilterParser.parse(filter, dync);
                filters.add(filt);
            }
        } catch (FilterException ex) {
            dynr.disconnect();
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("Could not parse filter rule >" + filter + "<: " + ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        } finally {
            dync.disconnect();
        }

        try {
            String json = dynr.get(includes, filters, size, page, order, countonly, unique, deflatt);
            rob.add("records", json);
            for(String curWarn: dynr.getWarnings()) {
                rob.addWarningMessage(curWarn);
            }
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get data: " + ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        } finally {
            dynr.disconnect();
        }
        rob.setStatus(Response.Status.OK);

        return rob.toResponseStream();
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

        // Init access
        DynRecords dynr;
        Configuration conf = new Configuration();
        try {
            if (conf.getProperty("mongo.url") != null) {
                dynr = new DynRecordsMongo();
            } else {
                dynr = new DynRecordsPostgres(storage, collection);
            }
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get data: " + ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        }

        try {
            dynr.update(json, id);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage(ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        } finally {
            dynr.disconnect();
        }

        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }

    @PUT
    @Path("{collection}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "Updates multiple datasets",
            description = "Updates existing datasets. The identity attribute must be included in the json.")
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

        // Init access
        DynRecords dynr;
        Configuration conf = new Configuration();
        try {
            if (conf.getProperty("mongo.url") != null) {
                dynr = new DynRecordsMongo();
            } else {
                dynr = new DynRecordsPostgres(storage, collection);
            }
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get data: " + ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        }

        try {
            dynr.update(json, null);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage(ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        } finally {
            dynr.disconnect();
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

        // Init access
        DynRecords dynr;
        Configuration conf = new Configuration();
        try {
            if (conf.getProperty("mongo.url") != null) {
                dynr = new DynRecordsMongo();
            } else {
                dynr = new DynRecordsPostgres(storage, collection);
            }
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get data: " + ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        }

        try {
            dynr.delete(id);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage(ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        } finally {
            dynr.disconnect();
        }

        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }
}
