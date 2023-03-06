package de.fhbielefeld.smartdata.rest;

import jakarta.ws.rs.core.Application;
import java.util.Set;

/**
 * Configuaration for REST interfaces
 * 
 * @author Florian Fehring
 */
@jakarta.ws.rs.ApplicationPath("smartdata")
public class ApplicationConfig extends Application {

    @Override
    public Set<Class<?>> getClasses() {
        Set<Class<?>> resources = new java.util.HashSet<>();
        addRestResourceClasses(resources);
        return resources;
    }

    /**
     * Do not modify addRestResourceClasses() method.
     * It is automatically populated with
     * all resources defined in the project.
     * If required, comment out calling this method in getClasses().
     */
    private void addRestResourceClasses(Set<Class<?>> resources) {
        resources.add(de.fhbielefeld.scl.rest.exceptions.handlers.GeneralExceptionMapper.class);
        resources.add(de.fhbielefeld.scl.rest.util.CORSFilter.class);
        resources.add(de.fhbielefeld.smartdata.rest.CollectionResource.class);
        resources.add(de.fhbielefeld.smartdata.rest.RecordsResource.class);
        resources.add(de.fhbielefeld.smartdata.rest.StorageResource.class);
        resources.add(de.fhbielefeld.smartdata.rest.SystemResource.class);
        resources.add(de.fhbielefeld.smartuser.rest.AuthenticationFilter.class);
        resources.add(de.fhbielefeld.smartuser.rest.OwnershipFilter.class);
    }
    
}
