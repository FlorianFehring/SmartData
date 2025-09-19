package de.fhbielefeld.smartdata.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartdata.config.Configuration;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import javax.naming.NamingException;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
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
            Configuration conf = new Configuration();
            Logger.getInstance("SmartData", moduleName);
            Logger.setDebugMode(Boolean.parseBoolean(conf.getProperty("debugmode")));
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

    @GET
    @Path("info")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get system information",
            description = "Gets information about the SmartData system and environment.")
    @APIResponse(
            responseCode = "200",
            description = "Objects with system informations",
            content = @Content(
                    mediaType = "application/json",
                    example = "{\"list\" : [ { \"name\" : \"value\"} ]}"
            ))
    @APIResponse(
            responseCode = "500",
            description = "Error mesage",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \" Could not get system informations: Because of ... \"]}"))
    public Response info() {
        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        // Init config
        Configuration conf = new Configuration();

        rob.add("version", "19.05.2023");
        // IP adress
        try {
            rob.add("publicip", InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException ex) {
            rob.add("publicip", "UnknwonIP Adress" + ex.getLocalizedMessage());
        }
        // MAC address
        String MAC = null;
        try {
            InetAddress localhost = InetAddress.getLocalHost();
            NetworkInterface ntInterface = NetworkInterface.getByInetAddress(localhost);
            if (ntInterface == null) {//Raspberry
                ntInterface = NetworkInterface.getByName("eth0");
            }
            if (ntInterface == null) {//Linux Server
                ntInterface = NetworkInterface.getByName("venet0");
            }
            if (ntInterface == null) {//Raspberry-WLAN
                ntInterface = NetworkInterface.getByName("wlan0");
            }
            if (ntInterface == null) {//No HW Address Found
                throw new UnknownHostException();
            }
            byte[] adr = ntInterface.getHardwareAddress();
            MAC = String.format("%02X:%02X:%02X:%02X:%02X:%02X", adr[0], adr[1], adr[2], adr[3], adr[4], adr[5]);
            rob.add("mac", MAC);
        } catch (UnknownHostException | SocketException | NullPointerException ex) {
            rob.add("mac", "Unknon MAC: " + ex.getLocalizedMessage());
        }
        //AppID
        if (conf.getProperty("appid") != null) {
            rob.add("appid", conf.getProperty("appid"));
        } else {
            rob.add("appid", MAC);
        }
        // HTTPS support check
        HttpURLConnection conn = null;
        try {
            URL httpsCheck = new URL("https://www.fh-bielefeld.de/");
            conn = (HttpURLConnection) httpsCheck.openConnection();
            conn.setConnectTimeout(1000);
            conn.connect();
            rob.add("httpsSupported", true);
        } catch (IOException ex) {
            rob.add("httpsSupported", false);
            System.err.println("DEBUG (SystemBean): There is no https support, because of: " + ex.getLocalizedMessage());
        } catch (NoSuchMethodError ex) {
            rob.add("httpsSupported", false);
            System.err.println("DEBUG (SystemBean): There is no https support, because of missing method: " + ex.getLocalizedMessage());
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        // Internet conectivity check
        HttpURLConnection con2 = null;
        try {
            URL httpsCheck = new URL("http://www.fh-bielefeld.de/");
            con2 = (HttpURLConnection) httpsCheck.openConnection();
            con2.setConnectTimeout(1000);
            con2.connect();
            rob.add("internetConnectivity", true);
        } catch (IOException ex) {
            rob.add("internetConnectivity", false);
            System.err.println("Could not connect to, because of: " + ex.getLocalizedMessage());
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        rob.add("systemJavaVendor", System.getProperty("java.vendor"));
        rob.add("systemJavaVersion", System.getProperty("java.version"));
        rob.add("systemName", System.getProperty("os.name"));
        String OS = System.getProperty("os.name").toLowerCase();
        if (OS.startsWith("win")) {
            rob.add("systemBase", "Windows");
        } else {
            rob.add("systemBase", "Linux");
        }
        rob.add("systemVersion", System.getProperty("os.version"));
        rob.add("systemArchitecture", System.getProperty("os.arch"));
        rob.setStatus(Response.Status.OK);

        return rob.toResponse();
    }

    @GET
    @Path("memorytest")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get heap usage",
            description = "Returns current heap usage of the JVM.")
    @APIResponse(
            responseCode = "200",
            description = "Heap usage information",
            content = @Content(
                    mediaType = "application/json",
                    example = "{\"usedHeapMB\" : 123, \"maxHeapMB\" : 512}"
            ))
    @APIResponse(
            responseCode = "500",
            description = "Error message",
            content = @Content(mediaType = "application/json",
                    example = "{\"errors\" : [ \"Could not retrieve heap info: ...\"]}"))
    public Response memoryTest(@Parameter(description = "If true provoke an OutOfMemoryError") @QueryParam("provokeOutOfMemory") boolean provokeOutOfMemory) {
        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        try {
            Runtime runtime = Runtime.getRuntime();
            long usedMemory = runtime.totalMemory() - runtime.freeMemory();
            long maxMemory = runtime.maxMemory();

            rob.add("usedHeapMB", usedMemory / (1024 * 1024));
            rob.add("maxHeapMB", maxMemory / (1024 * 1024));
            rob.setStatus(Response.Status.OK);
        } catch (Throwable t) {
            rob.addErrorMessage("Could not retrieve heap info: " + t.getLocalizedMessage());
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
        }

        try {
            if (provokeOutOfMemory == true) {
                List<byte[]> memoryHog = new ArrayList<>();
                while (true) {
                    memoryHog.add(new byte[1024 * 1024]); // 1 MB
                }
            }
        } catch (Throwable t) {
            Message msg = new Message("Unkown error occured: " + t.getLocalizedMessage(), MessageLevel.ERROR);
            Logger.addMessage(msg);
            throw t;
        }

        return rob.toResponse();
    }
}
