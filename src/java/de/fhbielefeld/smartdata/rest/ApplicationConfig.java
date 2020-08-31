/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.fhbielefeld.smartdata.rest;

import java.util.Set;
import javax.ws.rs.core.Application;

/**
 *
 * @author Florian Fehring
 */
@javax.ws.rs.ApplicationPath("smartdata")
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
        resources.add(de.fhbielefeld.smartdata.rest.BaseRessource.class);
        resources.add(de.fhbielefeld.smartdata.rest.RecordsResource.class);
        resources.add(de.fhbielefeld.smartdata.rest.TableResource.class);
    }
    
}
