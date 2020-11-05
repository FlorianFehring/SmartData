package de.fhbielefeld.smartdata.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartdata.dbo.Attribute;
import de.fhbielefeld.smartdata.dbo.DataCollection;
import de.fhbielefeld.smartdata.dyncollection.DynCollectionPostgres;
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
@Path("collection")
@Tag(name = "Collection", description = "Create and modify collections")
public class CollectionResource {

    /**
     * Creates a new instance of RootResource
     */
    public CollectionResource() {
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
    @Path("{collection}/create")
    @Operation(summary = "Creates a collection",
            description = "Creates a collection based on the definition given.")
    @APIResponse(
            responseCode = "201",
            description = "Collection created")
    @APIResponse(
            responseCode = "304",
            description = "Collection allready exists")
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not create collection: Because of ... \"]}"))
    public Response create(
            @Parameter(description = "Collections name", required = true, example = "mycollection") @PathParam("name") String name,
            @Parameter(description = "Storage name", required = false,
                    schema = @Schema(type = STRING, defaultValue = "public"),
                    example = "myschema") @QueryParam("storage") String storage,
            DataCollection collectiondef) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try {
            // Init collection access
            DynCollectionPostgres dtp = new DynCollectionPostgres(storage, collectiondef.getName());
            // Get attributes
            boolean created = dtp.create(collectiondef);
            if (created) {
                rob.setStatus(Response.Status.CREATED);
            } else {
                rob.setStatus(Response.Status.NOT_MODIFIED);
            }
            dtp.disconnect();
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get attribute information: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }
        return rob.toResponse();
    }

    @GET
    @Path("{collection}/getAttributes")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Lists attributes of a collection",
            description = "Lists all attributes of a collection and gives base information about them.")
    @APIResponse(
            responseCode = "200",
            description = "Objects with attribute informations",
            content = @Content(
                    mediaType = "application/json",
                    example = "{\"list\" : [ { \"name\" : \"attribute1\", \"type\" : \"integer\"} ]}"
            ))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not get datasets: Because of ... \"]}"))
    public Response getAttributes(
            @Parameter(description = "Collections name", required = true, example = "mycollection") @PathParam("collection") String collection,
            @Parameter(description = "Storage name", required = false,
                    schema = @Schema(type = STRING, defaultValue = "public"),
                    example = "mystorage") @QueryParam("storage") String storage) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try {
            // Init collection access
            DynCollectionPostgres dt = new DynCollectionPostgres(storage, collection);
            // Get attributes
            rob.add("list", dt.getAttributes().values());
            dt.disconnect();
            rob.setStatus(Response.Status.OK);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get attributes information: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }
        return rob.toResponse();
    }

    @PUT
    @Path("{collection}/addAttributes")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "Adds attributes",
            description = "Adds attributes to a collection.")
    @APIResponse(
            responseCode = "200",
            description = "Number of added attributes",
            content = @Content(
                    mediaType = "text/plain",
                    example = "1"
            ))
    @APIResponse(
            responseCode = "409",
            description = "Attribute allready exists",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Attribute 'attributename' allready exists. \"]}"))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not get datasets: Because of ... \"]}"))
    public Response addAttributes(
            @Parameter(description = "Collections name", required = true, example = "mycollection") @PathParam("collection") String collection,
            @Parameter(description = "Storage name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            List<Attribute> attributes) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try {
            // Init collection access
            DynCollectionPostgres dt = new DynCollectionPostgres(storage, collection);
            if (dt.addAttributes(attributes)) {
                rob.setStatus(Response.Status.CREATED);
            } else {
                rob.setStatus(Response.Status.CONFLICT);
            }
            dt.disconnect();
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get attribute information: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }

        return rob.toResponse();
    }

    @PUT
    @Path("{collection}/changeAttribute")
    @Operation(summary = "Changes the srid of a attribute",
            description = "Changes the srid of a attribute")
    @APIResponse(
            responseCode = "200",
            description = "SRID changed succsessfull")
    @APIResponse(
            responseCode = "404",
            description = "Attribute does not exists",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Attribute 'attributename' does not exists. \"]}")
    )
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not change SRID: Because of ... \"]}"))
    public Response changeAttribute(
            @Parameter(description = "Collections name", required = true, example = "mycollection") @PathParam("collection") String collection,
            @Parameter(description = "Storage name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            List<Attribute> attributes) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try {
            // Init collection access
            DynCollectionPostgres dt = new DynCollectionPostgres(storage, collection);
            dt.changeAttributes(attributes);
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