package de.fhbielefeld.smartdata.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartdata.config.Configuration;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import javax.naming.NamingException;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Path("proxy")
@Tag(name = "Proxy", description = "Universeller Proxy für externe HTTP-GET-Anfragen")
public class ProxyResource {

    public ProxyResource() {
        try {
            String moduleName = (String) new javax.naming.InitialContext().lookup("java:module/ModuleName");
            Configuration conf = new Configuration();
            Logger.getInstance("UniversalProxy", moduleName);
            Logger.setDebugMode(Boolean.parseBoolean(conf.getProperty("debugmode")));
        } catch (LoggerException | NamingException ex) {
            System.err.println("Fehler beim Initialisieren des Loggers: " + ex.getLocalizedMessage());
        }
    }

    @GET
    @Path("get")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Proxy-GET-Anfrage",
            description = "Führt eine HTTP-GET-Anfrage an eine beliebige URL mit optionalen Parametern aus")
    @APIResponse(
            responseCode = "200",
            description = "Antwort der Ziel-URL",
            content = @Content(mediaType = "application/json")
    )
    @APIResponse(
            responseCode = "500",
            description = "Fehlermeldung",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \"Fehler beim Proxy-Aufruf\"]}")
    )
    public Response proxyGet(@QueryParam("url") String url,
                             @Context UriInfo uriInfo) {
        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try {
            // Query-Parameter extrahieren (außer "url")
            MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
            StringBuilder paramBuilder = new StringBuilder();
            for (Map.Entry<String, List<String>> entry : queryParams.entrySet()) {
                if (!entry.getKey().equals("url")) {
                    for (String value : entry.getValue()) {
                        paramBuilder.append(URLEncoder.encode(entry.getKey(), "UTF-8"))
                                    .append("=")
                                    .append(URLEncoder.encode(value, "UTF-8"))
                                    .append("&");
                    }
                }
            }

            // Finalisierte URL mit Parametern
            String fullUrl = url + (paramBuilder.length() > 0 ? "?" + paramBuilder.toString() : "");
            if (fullUrl.endsWith("&")) fullUrl = fullUrl.substring(0, fullUrl.length() - 1);

            // HTTP-Verbindung aufbauen
            URL targetUrl = new URL(fullUrl);
            HttpURLConnection conn = (HttpURLConnection) targetUrl.openConnection();
            conn.setRequestMethod("GET");

            // Antwort lesen
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder responseBuilder = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    responseBuilder.append(line);
                }
                String jsonResponse = responseBuilder.toString();
                return Response.ok(jsonResponse, MediaType.APPLICATION_JSON).build();
            }

        } catch (IOException ex) {
            rob.addErrorMessage("Fehler beim Proxy-Aufruf: " + ex.getMessage());
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            return rob.toResponse();
        }
    }
}
