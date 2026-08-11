package de.fhbielefeld.smartdata.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartdata.config.Configuration;
import de.fhbielefeld.smartdata.dyn.DynFactory;
import de.fhbielefeld.smartdata.dyncollection.DynCollection;
import de.fhbielefeld.smartdata.dynrecords.DynRecords;
import de.fhbielefeld.smartdata.dynrecords.filter.EqualsFilter;
import de.fhbielefeld.smartdata.dynrecords.filter.Filter;
import de.fhbielefeld.smartdata.dynrecords.filter.FilterException;
import de.fhbielefeld.smartdata.exceptions.DynException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.naming.NamingException;
import org.json.JSONException;
import org.json.JSONArray;
import org.json.JSONObject;
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

    @POST
    @Path("ai/chat/completions")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Proxy AI chat request",
            description = "Forwards an OpenAI compatible chat request with server configured credentials.")
    @APIResponse(
            responseCode = "200",
            description = "Response from the configured AI service",
            content = @Content(mediaType = "application/json")
    )
    @APIResponse(
            responseCode = "503",
            description = "AI service is not configured",
            content = @Content(mediaType = "application/json")
    )
    public Response proxyAiChat(String json) {
        if (json == null || json.isBlank())
            return createAiError(Response.Status.BAD_REQUEST, "No AI request JSON was provided.");

        JSONObject request;
        try {
            request = new JSONObject(json);
        } catch (JSONException ex) {
            return createAiError(Response.Status.BAD_REQUEST, "AI request does not contain valid JSON.");
        }

        Configuration conf = new Configuration();
        String endpoint = conf.getProperty("ai.endpoint");
        String apiKey = getAiApiKey(conf);
        String model = conf.getProperty("ai.model");
        if (!hasText(endpoint) || !hasText(apiKey) || !hasText(model))
            return createAiError(Response.Status.SERVICE_UNAVAILABLE, "AI service is not configured.");

        request.put("model", model);
        request.remove("stream");
        byte[] requestBody = request.toString().getBytes(StandardCharsets.UTF_8);
        if (requestBody.length > getPositiveProperty(conf, "ai.maxRequestBytes", 2097152))
            return createAiError(Response.Status.REQUEST_ENTITY_TOO_LARGE, "AI request is too large.");

        HttpURLConnection conn = null;
        try {
            URL targetUrl = new URL(getAiChatEndpoint(endpoint));
            conn = (HttpURLConnection) targetUrl.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setConnectTimeout(getPositiveProperty(conf, "ai.timeout", 90000));
            conn.setReadTimeout(getPositiveProperty(conf, "ai.timeout", 90000));
            conn.setRequestProperty("Accept", MediaType.APPLICATION_JSON);
            conn.setRequestProperty("Content-Type", MediaType.APPLICATION_JSON + "; charset=UTF-8");
            conn.setRequestProperty("Authorization", "Bearer " + apiKey);
            try (OutputStream output = conn.getOutputStream()) {
                output.write(requestBody);
            }

            int status = conn.getResponseCode();
            String response = readResponse(status >= 400 ? conn.getErrorStream() : conn.getInputStream());
            return Response.status(status).type(MediaType.APPLICATION_JSON).entity(response).build();
        } catch (IOException ex) {
            return createAiError(Response.Status.BAD_GATEWAY, "AI service request failed: " + ex.getLocalizedMessage());
        } finally {
            if (conn != null)
                conn.disconnect();
        }
    }

    /**
     * Creates an OpenAI compatible error response.
     *
     * @param status HTTP response status
     * @param message Error message
     * @return Error response
     */
    private Response createAiError(Response.Status status, String message) {
        JSONObject error = new JSONObject();
        error.put("message", message);
        JSONObject body = new JSONObject();
        body.put("error", error);
        return Response.status(status).type(MediaType.APPLICATION_JSON).entity(body.toString()).build();
    }

    /**
     * Gets the OpenAI compatible chat endpoint.
     *
     * @param endpoint Configured API base URL
     * @return Chat completions endpoint
     */
    private String getAiChatEndpoint(String endpoint) {
        String normalized = endpoint.trim().replaceAll("/+$", "");
        normalized = normalized.replaceAll("/responses$", "");
        return normalized.endsWith("/chat/completions")
                ? normalized : normalized + "/chat/completions";
    }

    /**
     * Gets the configured AI API key.
     *
     * @param conf SmartData configuration
     * @return API key or null
     */
    private String getAiApiKey(Configuration conf) {
        String apiKey = conf.getProperty("ai.apiKey");
        if (hasText(apiKey))
            return apiKey.trim();

        String storage = getPropertyOrFallback(conf, "ai.configStorage", conf.getProperty("defaultstorage"));
        String collection = getPropertyOrFallback(conf, "ai.configCollection", "tbl_systemconfiguration");
        String key = getPropertyOrFallback(conf, "ai.configKey", "apikey_kiconnect");
        if (!hasText(storage))
            return null;

        List<Filter> filters = new ArrayList<>();
        try (DynCollection dync = DynFactory.getDynCollection(storage, collection)) {
            Filter keyFilter = new EqualsFilter(dync);
            keyFilter.parse("ckey,eq," + key);
            filters.add(keyFilter);

            Filter activeFilter = new EqualsFilter(dync);
            activeFilter.parse("active,eq,true");
            filters.add(activeFilter);
        } catch (DynException | FilterException ex) {
            logAiConfigurationError(ex);
            return null;
        }

        try (DynRecords dynr = DynFactory.getDynRecords(storage, collection)) {
            String json = dynr.get("cvalue", filters, 1, null, null, false, null, false, null, null, new ArrayList<>());
            JSONArray records = new JSONArray(json);
            if (records.isEmpty())
                return null;
            String databaseKey = records.getJSONObject(0).optString("cvalue", null);
            return hasText(databaseKey) ? databaseKey.trim() : null;
        } catch (DynException | JSONException ex) {
            logAiConfigurationError(ex);
            return null;
        }
    }

    /**
     * Gets a configured value with a fallback.
     *
     * @param conf SmartData configuration
     * @param key Configuration property name
     * @param fallback Fallback value
     * @return Configured value or fallback
     */
    private String getPropertyOrFallback(Configuration conf, String key, String fallback) {
        String value = conf.getProperty(key);
        return hasText(value) ? value.trim() : fallback;
    }

    /**
     * Logs a database configuration error without credentials.
     *
     * @param ex Configuration error
     */
    private void logAiConfigurationError(Exception ex) {
        Message message = new Message("ProxyResource", MessageLevel.WARNING,
                "Could not get AI API key from system configuration: " + ex.getLocalizedMessage());
        Logger.addDebugMessage(message);
    }

    /**
     * Reads a remote response body.
     *
     * @param input Response input stream
     * @return Response text
     * @throws IOException When the response cannot be read
     */
    private String readResponse(InputStream input) throws IOException {
        if (input == null)
            return "";
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null)
                response.append(line);
            return response.toString();
        }
    }

    /**
     * Gets a positive integer configuration value.
     *
     * @param conf SmartData configuration
     * @param key Configuration property name
     * @param fallback Fallback value
     * @return Positive configured value or fallback
     */
    private int getPositiveProperty(Configuration conf, String key, int fallback) {
        try {
            int value = Integer.parseInt(conf.getProperty(key));
            return value > 0 ? value : fallback;
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }

    /**
     * Checks whether a configuration value is available.
     *
     * @param value Configuration value
     * @return True when text is available
     */
    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
