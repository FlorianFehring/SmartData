package de.fhbielefeld.smartdata.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartdata.config.Configuration;
import de.fhbielefeld.smartdata.dbo.Attribute;
import de.fhbielefeld.smartdata.dbo.DataCollection;
import de.fhbielefeld.smartdata.dyn.DynFactory;
import de.fhbielefeld.smartdata.dyncollection.DynCollection;
import de.fhbielefeld.smartdata.exceptions.DynException;
import de.fhbielefeld.smartuser.annotations.SmartUserAuth;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.stream.JsonParser;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.StringReader;
import java.util.List;
import javax.naming.NamingException;
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

    public CollectionResource() {
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
    @Path("{collection}")
    @SmartUserAuth
    @Operation(summary = "Creates a collection",
            description = "Creates a collection based on the definition given.")
    @APIResponse(
            responseCode = "201",
            description = "Collection created")
    @APIResponse(
            responseCode = "304",
            description = "Collection allready exists")
    @APIResponse(
            responseCode = "400",
            description = "Collection definition is missing attributes")
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not create collection: Because of ... \"]}"))
    public Response create(
            @Parameter(description = "Collections name", required = true, example = "mycollection") @PathParam("collection") String name,
            @Parameter(description = "Storage name", required = false,
                    schema = @Schema(type = STRING, defaultValue = "public"),
                    example = "myschema") @QueryParam("storage") String storage,
            String collectiondefstr) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();
        
        JsonParser parser = Json.createParser(new StringReader(collectiondefstr));
        parser.next();
        JsonObject root = parser.getObject();

        // Set collections name from path param (do not use evtl. given name in json)
        DataCollection collectiondef = new DataCollection(name);

        JsonArray attrs = root.getJsonArray("attributes");
        for (int i = 0; i < attrs.size(); i++) {
            JsonObject attrdef = attrs.getJsonObject(i);
            if (attrdef == null) {
                continue;
            }
            Attribute attr = new Attribute();
            if (attrdef.containsKey("defaultValue")) {
                attr.setDefaultvalue(attrdef.getString("defaultValue"));
            }
            if (attrdef.containsKey("dimension")) {
                attr.setDimension(attrdef.getJsonNumber("dimension").intValue());
            }
            if (attrdef.containsKey("isAutoIncrement")) {
                attr.setIsAutoIncrement(attrdef.getBoolean("isAutoIncrement"));
            }
            if (attrdef.containsKey("isNullable")) {
                attr.setIsNullable(attrdef.getBoolean("isNullable"));
            }
            if (attrdef.containsKey("isIdentity")) {
                attr.setIsIdentity(attrdef.getBoolean("isIdentity"));
            }
            // Check for missing name
            if(!attrdef.containsKey("name") || attrdef.getString("name").isEmpty()) {
                var resp = Response.Status.CONFLICT;
                rob.setStatus(resp);
                
                rob.addErrorMessage("Name is missing for column.");
                return rob.toResponse();
            }
            attr.setName(attrdef.getString("name"));
            if (attrdef.containsKey("refAttribute")) {
                attr.setRefAttribute(attrdef.getString("refAttribute"));
            }
            if (attrdef.containsKey("refCollection")) {
                attr.setRefCollection(attrdef.getString("refCollection"));
            }
            if (attrdef.containsKey("refStorage")) {
                attr.setRefStorage(attrdef.getString("refStorage"));
            }
            if (attrdef.containsKey("refOnDelete")) {
                attr.setRefOnDelete(attrdef.getString("refOnDelete"));
            }
            if (attrdef.containsKey("refOnUpdate")) {
                attr.setRefOnUpdate(attrdef.getString("refOnUpdate"));
            }
            if (attrdef.containsKey("srid")) {
                attr.setSrid(attrdef.getJsonNumber("srid").intValue());
            }
            if (attrdef.containsKey("subtype")) {
                attr.setSubtype(attrdef.getString("subtype"));
            }
            if (attrdef.containsKey("type")) {
                attr.setType(attrdef.getString("type"));
            }
            collectiondef.addAttribute(attr);
        }

        if (collectiondef.getAttributes() == null || collectiondef.getAttributes().isEmpty()) {
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("The collection definition does not contain attributes.");
            return rob.toResponse();
        }

        try ( DynCollection dync = DynFactory.getDynCollection(storage, collectiondef.getName())) {
            // Get attributes
            boolean created = dync.create(collectiondef);
            if (created) {
                rob.setStatus(Response.Status.CREATED);
            } else {
                rob.setStatus(Response.Status.NOT_MODIFIED);
            }
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get attribute information: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }
        return rob.toResponse();
    }

    @GET
    @Path("{collection}")
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Gets the definition of the collection",
            description = "Lists all attributes of a collection and gives base information about them.")
    @APIResponse(
            responseCode = "200",
            description = "Objects with attribute informations",
            content = @Content(
                    mediaType = "application/json",
                    example = "{\"attributes\" : [ { \"name\" : \"attribute1\", \"type\" : \"integer\"} ]}"
            ))
    @APIResponse(
            responseCode = "404",
            description = "Table does not exists, or the whole schema does not exists.",
            content = @Content(
                    mediaType = "application/json",
                    example = "{\"errors\" : [ { \"Table or schema does not exists\"]}"
            ))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not get datasets: Because of ... \"]}"))
    public Response get(
            @Parameter(description = "Collections name", required = true, example = "mycollection") @PathParam("collection") String collection,
            @Parameter(description = "Storage name", required = false,
                    schema = @Schema(type = STRING, defaultValue = "public"),
                    example = "mystorage") @QueryParam("storage") String storage) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try ( DynCollection dync = DynFactory.getDynCollection(storage, collection)) {
            // Get attributes
            rob.add("name", collection);
            rob.add("storage", storage);
            rob.add("attributes", dync.getAttributes().values());
            rob.setStatus(Response.Status.OK);
        } catch (DynException ex) {
            System.out.println(ex.getLocalizedMessage());
            if (ex.getLocalizedMessage().contains("does not exists")) {
                rob.setStatus(Response.Status.NOT_FOUND);
            } else {
                rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            }
            rob.addErrorMessage("Could not get attributes information: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }
        return rob.toResponse();
    }

    @PUT
    @Path("{collection}/addAttributes")
    @Consumes(MediaType.APPLICATION_JSON)
    @SmartUserAuth
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

        try ( DynCollection dync = DynFactory.getDynCollection(storage, collection)) {
            if (dync.addAttributes(attributes)) {
                rob.setStatus(Response.Status.CREATED);
            } else {
                rob.setStatus(Response.Status.CONFLICT);
            }
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get attribute information: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }

        return rob.toResponse();
    }

    @PUT
    @Path("{collection}/delAttributes")
    @Consumes(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Deletes attributes",
            description = "Delets attributes from a collection.")
    @APIResponse(
            responseCode = "200",
            description = "Number of deleted attributes",
            content = @Content(
                    mediaType = "text/plain",
                    example = "1"
            ))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not get datasets: Because of ... \"]}"))
    public Response delAttributes(
            @Parameter(description = "Collections name", required = true, example = "mycollection") @PathParam("collection") String collection,
            @Parameter(description = "Storage name",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            List<Attribute> attributes) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try ( DynCollection dync = DynFactory.getDynCollection(storage, collection)) {
            if (dync.delAttributes(attributes)) {
                rob.setStatus(Response.Status.CREATED);
            } else {
                rob.setStatus(Response.Status.CONFLICT);
            }
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get attribute information: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }

        return rob.toResponse();
    }
    
    @PUT
    @Path("{collection}/changeAttribute")
    @SmartUserAuth
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

        try ( DynCollection dync = DynFactory.getDynCollection(storage, collection)) {
            dync.changeAttributes(attributes);
            rob.setStatus(Response.Status.OK);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not change SRID: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }

        return rob.toResponse();
    }

    @DELETE
    @Path("{collection}")
    @SmartUserAuth
    @Operation(summary = "Deletes a collection",
            description = "Delets the given collection and all of its contents.")
    @APIResponse(
            responseCode = "200",
            description = "Collection deleted")
    @APIResponse(
            responseCode = "304",
            description = "Collection does not exists")
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not create collection: Because of ... \"]}"))
    public Response delete(
            @Parameter(description = "Collections name", required = true, example = "mycollection") @PathParam("collection") String collection,
            @Parameter(description = "Storage name", required = false,
                    schema = @Schema(type = STRING, defaultValue = "public"),
                    example = "myschema") @QueryParam("storage") String storage) {

        if (storage == null) {
            storage = "public";
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try ( DynCollection dync = DynFactory.getDynCollection(storage, collection)) {
            dync.delete();
            rob.setStatus(Response.Status.OK);
        } catch (DynException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not delete collection >" + collection + "<: " + ex.getLocalizedMessage());
            rob.addException(ex);
        }
        return rob.toResponse();
    }
}
