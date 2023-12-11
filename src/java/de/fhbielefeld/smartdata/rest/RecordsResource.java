package de.fhbielefeld.smartdata.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartdata.config.Configuration;
import de.fhbielefeld.smartdata.dbo.Attribute;
import de.fhbielefeld.smartdata.dyn.DynFactory;
import de.fhbielefeld.smartdata.dyncollection.DynCollection;
import de.fhbielefeld.smartdata.dyncollection.DynCollectionMongo;
import de.fhbielefeld.smartdata.dynrecords.filter.EqualsFilter;
import de.fhbielefeld.smartdata.dynrecords.filter.Filter;
import de.fhbielefeld.smartdata.dynrecords.filter.FilterException;
import de.fhbielefeld.smartdata.dynrecords.filter.FilterParser;
import de.fhbielefeld.smartdata.dyncollection.DynCollectionPostgres;
import de.fhbielefeld.smartdata.dynrecords.DynRecords;
import de.fhbielefeld.smartdata.exceptions.DynException;
import de.fhbielefeld.smartuser.annotations.SmartUserAuth;
import de.fhbielefeld.smartuser.securitycontext.SmartPrincipal;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import javax.naming.NamingException;
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

    private static Map<String, DynCollection> dynColCache = new HashMap<>();
    private static Map<String, DynRecords> dynRecCache = new HashMap<>();

    public RecordsResource() {
        // Init logging
        try {
            String moduleName = (String) new javax.naming.InitialContext().lookup("java:module/ModuleName");
            Configuration conf = new Configuration();
            Logger.getInstance("SmartData", moduleName);
            Logger.setDebugMode(Boolean.parseBoolean(conf.getProperty("debugmode")));
        } catch (LoggerException | NamingException ex) {
            System.err.println("Error init logger: " + ex.getLocalizedMessage());
        }
    }

    @POST
    @Path("{collection}/")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Creates a new dataset",
            description = "Creates a new dataset stored in database")
    @APIResponse(
            responseCode = "201",
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

        Configuration conf = new Configuration();
        try (DynRecords dynr = DynFactory.getDynRecords(storage, collection)) {
            List<Object> ids = dynr.create(json);
            // Use TreeQL specification extension
            if (conf.getProperty("spec.version") != null
                    && conf.getProperty("spec.version").equals("2020fhbi")) {
                DynCollection dync;
                if (conf.getProperty("mongo.url") != null) {
                    dync = new DynCollectionMongo(storage, collection);
                } else {
                    dync = new DynCollectionPostgres(storage, collection);
                }
                Attribute pkattr = dync.getIdentityAttributes().get(0);
                rob.add(pkattr.getName(), dynr.create(json));
                for (String curWarning : dynr.getWarnings()) {
                    rob.addWarningMessage(curWarning);
                }
                rob.setStatus(Response.Status.CREATED);
            } else {
                if (!dynr.getWarnings().isEmpty()) {
                    String msgtxt = "Warnings occured, that can't be deliverd";
                    for (String curWarning : dynr.getWarnings()) {
                        msgtxt += System.lineSeparator() + "- " + curWarning;
                    }
                    Message msg = new Message(msgtxt, MessageLevel.WARNING);
                    Logger.addDebugMessage(msg);
                }
                Response.ResponseBuilder rb = Response.status(Response.Status.CREATED);
                String idstr = ids.toString().replace("[", "").replace("]", "").replace(" ", "");
                rb.entity(idstr);
                return rb.build();
            }
        } catch (DynException ex) {
            if (ex.getLocalizedMessage().contains("Unique-Constraint")) {
                rob.setStatus(Response.Status.CONFLICT);
            } else if (ex.getLocalizedMessage().contains("does not exists")) {
                rob.setStatus(Response.Status.NOT_FOUND);
            } else {
                rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            }
            rob.addErrorMessage("Could not create dataset: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }
        return rob.toResponse();
    }

    @GET
    @Path("{collection}/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
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
            @Parameter(description = "Attributes to include, comata separated", example = "id,value") @QueryParam("includes") String includes,
            @Parameter(description = "Name of the geo column that contains geo information, for reciving the data in geojson format",
                    schema = @Schema(type = STRING)) @QueryParam("geojsonattr") String geojsonattr,
            @Parameter(description = "Coordinate system in which geometry information schould be deliverd. Can be an EPSG code or 'latlon'") @QueryParam("geotransform") String geotransform,
            @Parameter(description = "Package values into datasets") @QueryParam("deflatt") boolean deflatt) {

        if (storage == null) {
            storage = "public";
        }
//        long start = System.nanoTime();
        ResponseObjectBuilder rob = new ResponseObjectBuilder();

//        long stopInitAccess = System.nanoTime();
//        double neededInitAccess = stopInitAccess - startInitAccess;
//        double neededInitAccessA = neededInitAccess / 1000 / 1000;
//        System.out.println("Time for initAccess: " + neededInitAccessA + " ms");
        List<Filter> filters = new ArrayList<>();
        // Init collection access
//        long startIdFilter = System.nanoTime();
        try (DynCollection dync = DynFactory.getDynCollection(storage, collection)) {
            List<Attribute> idattrs = dync.getIdentityAttributes();

            if (idattrs.isEmpty()) {
                rob.addErrorMessage("There is no identity attribute for collection >" + collection + "< could not get single dataset.");
                rob.setStatus(Response.Status.NOT_ACCEPTABLE);
                return rob.toResponse();
            } else if (idattrs.size() > 1) {
                rob.addWarningMessage("There are more than one identity attributes, try to identify on >" + idattrs.get(0).getName() + "<");
            }
            // Create filter for id
            Attribute idattr = idattrs.get(0);
            Filter idfilter = new EqualsFilter(dync);
            idfilter.parse(idattr.getName() + ",eq," + id);
            filters.add(idfilter);

        } catch (DynException ex) {
            rob.setStatus(Response.Status.NOT_ACCEPTABLE);
            rob.addErrorMessage("Could not get identity attributes");
            rob.addException(ex);
            return rob.toResponse();
        } catch (FilterException ex) {
            rob.setStatus(Response.Status.NOT_ACCEPTABLE);
            rob.addErrorMessage("Could not create filter for id");
            rob.addException(ex);
            return rob.toResponse();
        }
//        long stopIdFilter = System.nanoTime();
//        double neededIdFilter = stopIdFilter - startIdFilter;
//        double neededIdFilterB = neededIdFilter / 1000 / 1000;
//        System.out.println("Time for IdFilter: " + neededIdFilterB + " ms");

//        long startBuildResponse=0;
//        long startGetData = System.nanoTime();
        try (DynRecords dynr = DynFactory.getDynRecords(storage, collection)) {
            String json = dynr.get(includes, filters, 1, null, null, false, null, deflatt, geojsonattr, geotransform, new ArrayList<>());
//            long finishGetData = System.nanoTime();
//            double neededTimes = finishGetData - startGetData;
//            double needetGetData = neededTimes / 1000 / 1000;
//            System.out.println("Time for get data: " + needetGetData + " ms");
            // Convert to utf8
            byte[] u8 = json.getBytes(StandardCharsets.UTF_8);
            if (geojsonattr != null) {
                Response.ResponseBuilder rb = Response.status(Response.Status.OK);
                rb.entity(json);
                // Add HATEOAS links
                rob.addLink("records/" + collection + "/" + id, "edit");
                rob.addLink("records/" + collection + "/" + id, "delete");
                rob.addLink("records/" + collection + "/" + (id + 1), "next");
                if (id > 1) {
                    rob.addLink("records/" + collection + "/" + (id - 1), "prev");
                }
                return rb.build();
            } else {
                rob.add("records", new String(u8));
            }
            for (String curWarn : dynr.getWarnings()) {
                rob.addWarningMessage(curWarn);
            }
        } catch (DynException ex) {
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("Could not get data: " + ex.getLocalizedMessage());
            rob.addException(ex);
            return rob.toResponse();
        }
        rob.setStatus(Response.Status.OK);

//        long finish = System.nanoTime();
//        double finishDouble = finish - start;
//        double needetFinish = finishDouble / 1000 / 1000;
//        System.out.println("Time until response: " + needetFinish + " ms");
        return rob.toResponseStream();
    }

    @GET
    @Path("{collection}/")
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
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
            responseCode = "404",
            description = "Table not found")
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
            @Parameter(description = "Definition of an filter <a href=\"http://git04-ifm-min.ad.fh-bielefeld.de/forschung/smartecosystem/smartdata/-/wikis/Funktionen/Uebersicht\" target=\"_new\">See filter documentation</a>", example = "id,eq,1") @QueryParam("filter") List<String> filtersStrings,
            @Parameter(description = "Maximum number of datasets", example = "1") @QueryParam("size") int size,
            @Parameter(description = "Page no to recive", example = "1") @QueryParam("page") String page,
            @Parameter(description = "Datasets order column and order kind", example = "column[,desc]") @QueryParam("order") String order,
            @Parameter(description = "If datasets should only be counted") @QueryParam("countonly") boolean countonly,
            @Parameter(description = "Attribute to get uniqe values for (untested)", example = "value") @QueryParam("unique") String unique,
            @Parameter(description = "Name of the geo column that contains geo information, for reciving the data in geojson format") @QueryParam("geojsonattr") String geojsonattr,
            @Parameter(description = "Coordinate system in which geometry information schould be deliverd. Can be an EPSG code or 'latlon'") @QueryParam("geotransform") String geotransform,
            @Parameter(description = "Names of join tables, to make natural join.", example = "bindtable,endtable") @QueryParam("join") List<String> joins,
            @Context ContainerRequestContext requestContext) {

        if (storage == null) {
            storage = "public";
        }

        // Catch negative limits
        if (size < 0) {
            size = 0;
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();
        List<Filter> filters = new ArrayList<>();

        try (DynCollection dync = DynFactory.getDynCollection(storage, collection)) {
            // Check if there is a request context and user has restricted rights
            if (requestContext != null) {
                boolean max_right = false;
                String contextInfo = null;
                SecurityContext sc = requestContext.getSecurityContext();
                if (sc != null) {
                    SmartPrincipal sp = (SmartPrincipal) sc.getUserPrincipal();
                    if (sp != null) {
                        StringJoiner sb = new StringJoiner(",");
                        for (Long curSet : sp.getContextRight().getIds()) {
                            if (curSet == Long.MAX_VALUE) {
                                max_right = true;
                                break;
                            }
                            sb.add(curSet.toString());
                        }
                        // Get identity column (only first identity supported)
                        Attribute idattr = dync.getIdentityAttributes().get(0);
                        if (sb.length() > 0) {
                            // Write filter
                            filtersStrings.add(idattr.getName() + ",in," + sb.toString());
                        } else if (!max_right) {
                            // User has no right so shold get a empty list
                            filtersStrings.add(idattr.getName() + ",in,-1");
                        }
                    }
                } else {
                    contextInfo = "No SecurityContext in Requestcontext found!";
                    Message msg = new Message(contextInfo, MessageLevel.INFO);
                    Logger.addDebugMessage(msg);
                    rob.setStatus(Response.Status.UNAUTHORIZED);
                    return rob.toResponse();
                }
            } // End security check
            // Build filters from strings given by parameter or by security context
            if (filtersStrings != null && !filtersStrings.isEmpty()) {
                try {
                    // Build filter objects
                    for (String curFilterStr : filtersStrings) {
                        Filter filt = FilterParser.parse(curFilterStr, dync);
                        if (filt != null) {
                            filters.add(filt);
                        }
                    }
                } catch (FilterException ex) {
                    rob.setStatus(Response.Status.BAD_REQUEST);
                    rob.addErrorMessage("Could not parse filter rule >" + filtersStrings + "<: " + ex.getLocalizedMessage());
                    rob.addException(ex);
                    return rob.toResponse();
                }
            }
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get identity column.");
            return rob.toResponse();
        }

        try (DynRecords dynr = DynFactory.getDynRecords(storage, collection)) {
            String json = dynr.get(includes, filters, size, page, order, countonly, unique, false, geojsonattr, geotransform, joins);
            if (json.equals("{}")) {
                json = "[]";
            }
            // Convert to utf8
            byte[] u8 = json.getBytes(StandardCharsets.UTF_8);
            if (geojsonattr != null) {
                Response.ResponseBuilder rb = Response.status(Response.Status.OK);
                rb.entity(json);
                return rb.build();
            } else {
                rob.add("records", new String(u8));
            }
            for (String curWarn : dynr.getWarnings()) {
                rob.addWarningMessage(curWarn);
            }
        } catch (DynException ex) {
            String msg = ex.getLocalizedMessage();
            if ((msg.contains("Tabelle") && msg.contains("existiert nicht"))
                    || (msg.contains("Table") && msg.contains("does not exist"))) {
                rob.setStatus(Response.Status.NOT_FOUND);
                String msg1 = msg + " Check table ownership.";
                rob.addErrorMessage(msg1);
                Message msge = new Message(msg1, MessageLevel.ERROR);
                Logger.addDebugMessage(msge);
            } else if ((msg.contains("Spalte") && msg.contains(" existiert nicht"))
                    || (msg.contains("Column") && msg.contains("does not exist"))) {
                rob.setStatus(Response.Status.NOT_FOUND);
                String msg1 = msg + " Did you deleted a column from database? Than restart of SmartData is requeired.";
                rob.addErrorMessage(msg1);
                Message msge = new Message(msg1, MessageLevel.ERROR);
                Logger.addDebugMessage(msge);
            } else {
                rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
                rob.addErrorMessage("Could not get data: " + msg);
                rob.addException(ex);
            }
            return rob.toResponse();
        }
        rob.setStatus(Response.Status.OK);

        return rob.toResponseStream();
    }

    @PUT
    @Path("{collection}/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @SmartUserAuth
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
            @Parameter(description = "Collections name", required = true, example = "mycollection")
            @PathParam("collection") String collection,
            @Parameter(description = "Datasets id", required = true, example = "1") @PathParam("id") Long id,
            @Parameter(description = "Storage name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            @Parameter(description = "json data",
                    schema = @Schema(type = STRING, defaultValue = "public")) String json
    ) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try (DynRecords dynr = DynFactory.getDynRecords(storage, collection)) {
            dynr.update(json, id);
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
    @SmartUserAuth
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
            @Parameter(description = "Collections name", required = true, example = "mycollection")
            @PathParam("collection") String collection,
            @Parameter(description = "Storage name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            @Parameter(description = "json data",
                    schema = @Schema(type = STRING, defaultValue = "public")) String json
    ) {

        if (storage == null) {
            storage = "public";
        }
        
        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        if (json == null || json.isEmpty()) {
            rob.setStatus(Response.Status.NOT_ACCEPTABLE);
            rob.addErrorMessage("No data to update.");
            return rob.toResponse();
        }
         
        try (DynRecords dynr = DynFactory.getDynRecords(storage, collection)) {
            dynr.update(json, null);
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
    @SmartUserAuth
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
            @Parameter(description = "Collections name", required = true, example = "mycollection")
            @PathParam("collection") String collection,
            @Parameter(description = "Storage name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            @Parameter(description = "Dataset id", required = true, example = "1") @PathParam("id") String id
    ) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try (DynRecords dynr = DynFactory.getDynRecords(storage, collection)) {
            dynr.delete(id);
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
