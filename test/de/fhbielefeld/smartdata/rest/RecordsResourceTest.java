package de.fhbielefeld.smartdata.rest;

import de.fhbielefeld.scl.rest.util.WebTargetCreator;
import java.time.LocalDateTime;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for the Records resource
 * 
 * @author Florian Fehring
 */
public class RecordsResourceTest {
    
    private static LocalDateTime startDateTime;
    private static final String SERVER = "http://localhost:8080/SmartData/smartdata/";
    private static final String RESOURCE = "records";
    private static final String SCHEMA = "test";
    private static WebTarget webTarget;
    private static final boolean PRINT_DEBUG_MESSAGES = true;
    
    public RecordsResourceTest() {        
    }
    
    @BeforeClass
    public static void setUpClass() {
        startDateTime = LocalDateTime.now();
        System.out.println("TEST beforeAll");
        webTarget = WebTargetCreator.createWebTarget(SERVER,RESOURCE);
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void testGetOne() {
        if(webTarget == null)
            System.out.println("WebTarget is null! Änderung?");
        
        WebTarget target = webTarget.path("testdaten")
                .path("1");
        Response response = target.request(MediaType.APPLICATION_JSON).get();
        String responseText = response.readEntity(String.class);
        if (PRINT_DEBUG_MESSAGES) {
            System.out.println("---testGetOne---");
            System.out.println(response.getStatusInfo());
            System.out.println(responseText);
        }
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }
}
