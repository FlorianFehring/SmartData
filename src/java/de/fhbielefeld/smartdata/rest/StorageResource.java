package de.fhbielefeld.smartdata.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartdata.config.Configuration;
import de.fhbielefeld.smartdata.dyn.DynFactory;
import de.fhbielefeld.smartdata.exceptions.DynException;
import javax.naming.NamingException;
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
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import de.fhbielefeld.smartdata.dynstorage.DynStorage;
import de.fhbielefeld.smartuser.annotations.SmartUserAuth;

/**
 * Resource for accessing database informations
 *
 * @author Florian Fehring
 */
@Path("storage")
@Tag(name = "Storage", description = "Manage data storages")
public class StorageResource {

    /**
     * Creates a new instance of RootResource
     */
    public StorageResource() {
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
    @Path("create")
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Creates a new storage",
            description = "Creates a new storage")
    @APIResponse(
            responseCode = "201",
            description = "Storage was created")
    @APIResponse(
            responseCode = "304",
            description = "Storage already exists")
    @APIResponse(
            responseCode = "400",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \"No storage name given.\"]}"))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not create storage: Because of ... \"]}"))
    public Response create(@Parameter(description = "Storage name", required = true,
            schema = @Schema(type = STRING, defaultValue = "public")
    ) @QueryParam("name") String name) {
        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        if (name == null) {
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("No storage name given.");
            return rob.toResponse();
        }

        try(DynStorage dyns = DynFactory.getDynStorage()) {
            if (dyns.createStorageIfNotExists(name)) {
                rob.setStatus(Response.Status.CREATED);
            } else {
                rob.setStatus(Response.Status.NOT_MODIFIED);
            }
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Error retriving collection names: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }

        return rob.toResponse();
    }

    @GET
    @Path("getAbilities")
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Lists all abilities",
            description = "Lists all abilities of a storage")
    @APIResponse(
            responseCode = "200",
            description = "List with ability informations",
            content = @Content(
                    mediaType = "application/json",
                    example = "{ \"list\" : { \"ability_name\" : \"ability_version\"}}"
            ))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not get abilities: Because of ... \"]}"))
    public Response getAbilities(
            @Parameter(description = "Storage name", required = false,
                    schema = @Schema(type = STRING, defaultValue = "public")
            ) @QueryParam("name") String name) {

        if (name == null) {
            name = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try(DynStorage dyns = DynFactory.getDynStorage()) {
            rob.add("list", dyns.getAbilities());
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Error retriving collection names: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }

        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }
    
    @GET
    @Path("getCollections")
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Lists all collections",
            description = "Lists all collections of a storage")
    @APIResponse(
            responseCode = "200",
            description = "List with collection informations",
            content = @Content(
                    mediaType = "application/json",
                    example = "{ \"list\" : { \"name\" : \"collection\"}}"
            ))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not get collections: Because of ... \"]}"))
    public Response getCollections(
            @Parameter(description = "Storage name", required = false,
                    schema = @Schema(type = STRING, defaultValue = "public")
            ) @QueryParam("name") String name) {

        if (name == null) {
            name = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try(DynStorage dyns = DynFactory.getDynStorage()) {
            rob.add("list", dyns.getCollections(name));
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Error retriving collection names: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }

        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }

    @DELETE
    @Path("delete")
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Deletes a storage",
            description = "Deletes a storage and all its contents")
    @APIResponse(
            responseCode = "200",
            description = "Storage was deleted")
    @APIResponse(
            responseCode = "304",
            description = "Storage was not existend")
    @APIResponse(
            responseCode = "400",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \"No storage name given.\"]}"))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not delete storage: Because of ... \"]}"))
    public Response delete(@Parameter(description = "Storage name", required = true,
            schema = @Schema(type = STRING, defaultValue = "public")
    ) @QueryParam("name") String name) {
        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        if (name == null) {
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("No storage name given.");
            return rob.toResponse();
        }

        try(DynStorage dyns = DynFactory.getDynStorage()) {
            if (dyns.deleteStorage(name)) {
                rob.setStatus(Response.Status.OK);
            } else {
                rob.setStatus(Response.Status.NOT_MODIFIED);
            }
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Error retriving collection names: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }

        return rob.toResponse();
    }
}
