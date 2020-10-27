package de.fhbielefeld.smartdata.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartdata.config.Configuration;
import de.fhbielefeld.smartdata.dyncollection.DynCollectionPostgres;
import de.fhbielefeld.smartdata.exceptions.DynException;
import java.util.Map.Entry;
import javax.naming.NamingException;
import javax.ws.rs.Produces;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
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
 * REST Web Service for accessing system options and informations
 *
 * @author Florian Fehring
 */
@Path("system")
@Tag(name = "System", description = "SmartData system informations and configuration")
public class SystemResource {

    /**
     * Creates a new instance of RootResource
     */
    public SystemResource() {
        // Init logging
        try {
            String moduleName = (String) new javax.naming.InitialContext().lookup("java:module/ModuleName");
            Logger.getInstance("SmartData", moduleName);
            Logger.setDebugMode(true);
        } catch (LoggerException | NamingException ex) {
            System.err.println("Error init logger: " + ex.getLocalizedMessage());
        }
    }

    @GET
    @Path("config")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get configuration",
            description = "Lists all configuration options.")
    @APIResponse(
            responseCode = "200",
            description = "Objects with configuration informations",
            content = @Content(
                    mediaType = "application/json",
                    example = "{\"list\" : [ { \"name\" : \"value\"} ]}"
            ))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not load configuration: Because of ... \"]}"))
    public Response getConfig() {
        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        // Init config
        Configuration conf = new Configuration();

        rob.add("modulname", conf.getModuleName());
        rob.add("filename", conf.getFileName());
        rob.add("propsloaded", conf.isPropsloaded());
        for (Entry<Object, Object> curEntry : conf.getAllProperties()) {
            rob.add(curEntry.getKey().toString(), curEntry.getValue().toString());
        }
        rob.setStatus(Response.Status.OK);

        return rob.toResponse();
    }
}
