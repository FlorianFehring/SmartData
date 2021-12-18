package de.fhbielefeld.smartdata.exceptions;

/**
 * Exception reporting errors within Dyn operations
 * 
 * @author Florian Fehring
 */
public class DynException extends Exception {
    
    public DynException(String message) {
        super(message);
    }
}
