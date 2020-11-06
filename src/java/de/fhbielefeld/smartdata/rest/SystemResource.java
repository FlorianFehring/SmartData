package de.fhbielefeld.smartdata.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartdata.config.Configuration;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.util.Map.Entry;
import javax.naming.NamingException;
import javax.ws.rs.Produces;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
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

    @GET
    @Path("sysinfo")
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
    public Response getSysinfo() {
        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        // Init config
        Configuration conf = new Configuration();

        rob.add("version", "06.11.2020");
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
        try {
            URL masterURL = new URL("https://google.de");
            final URLConnection conn = masterURL.openConnection();
            conn.setConnectTimeout(1000);
            conn.connect();
            rob.add("httpsSupported", true);
        } catch (IOException ex) {
            rob.add("httpsSupported", false);
            System.err.println("DEBUG (SystemBean): There is no https support, because of: " + ex.getLocalizedMessage());
        } catch (NoSuchMethodError ex) {
            rob.add("httpsSupported", false);
            System.err.println("DEBUG (SystemBean): There is no https support, because of missing method: " + ex.getLocalizedMessage());
        }
        // Internet conectivity check
        try {
            final URL url = new URL("http://www.google.de");
            final URLConnection conn = url.openConnection();
            conn.setConnectTimeout(1000);
            conn.connect();
            rob.add("internetConnectivity", true);
        } catch (IOException ex) {
            rob.add("internetConnectivity", false);
            System.err.println("Could not connect to google.de " + ex.getLocalizedMessage());
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
}
