package de.fhbielefeld.smartdata.dyndata.filter;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.smartdata.dyntable.DynTable;

/**
 *
 * @author Florian Fehring
 */
public class FilterParser {

    public static Filter parse(String filter, DynTable table) throws FilterException {
        Filter f = null;
        if (filter.contains(",cs,")) {
            Message msg = new Message("SmartData", MessageLevel.ERROR, "Filter for >" + filter + "< is not implemented yet.");
            Logger.addMessage(msg);
        } else if (filter.contains(",sw,")) {
            Message msg = new Message("SmartData", MessageLevel.ERROR, "Filter for >" + filter + "< is not implemented yet.");
            Logger.addMessage(msg);
        } else if (filter.contains(",ew,")) {
            Message msg = new Message("SmartData", MessageLevel.ERROR, "Filter for >" + filter + "< is not implemented yet.");
            Logger.addMessage(msg);
        } else if (filter.contains(",eq,")) {
            f = new EqualsFilter(table);
            f.parse(filter);
        } else if (filter.contains(",lt,")) {
            Message msg = new Message("SmartData", MessageLevel.ERROR, "Filter for >" + filter + "< is not implemented yet.");
            Logger.addMessage(msg);
        } else if (filter.contains(",le,")) {
            Message msg = new Message("SmartData", MessageLevel.ERROR, "Filter for >" + filter + "< is not implemented yet.");
            Logger.addMessage(msg);
        } else if (filter.contains(",ge,")) {
            Message msg = new Message("SmartData", MessageLevel.ERROR, "Filter for >" + filter + "< is not implemented yet.");
            Logger.addMessage(msg);
        } else if (filter.contains(",gt,")) {
            Message msg = new Message("SmartData", MessageLevel.ERROR, "Filter for >" + filter + "< is not implemented yet.");
            Logger.addMessage(msg);
        } else if (filter.contains(",bt,")) {
            Message msg = new Message("SmartData", MessageLevel.ERROR, "Filter for >" + filter + "< is not implemented yet.");
            Logger.addMessage(msg);
        } else if (filter.contains(",in,")) {
            Message msg = new Message("SmartData", MessageLevel.ERROR, "Filter for >" + filter + "< is not implemented yet.");
            Logger.addMessage(msg);
        } else if (filter.contains(",is,")) {
            Message msg = new Message("SmartData", MessageLevel.ERROR, "Filter for >" + filter + "< is not implemented yet.");
            Logger.addMessage(msg);
        } else if (filter.contains(",sir,")) {
            f = new RadiusFilter(table);
            f.parse(filter);
        } else if (filter.contains(",sib,")) {
            f = new BoundingBoxFilter(table);
            f.parse(filter); 
        } else {
            Message msg = new Message("SmartData", MessageLevel.ERROR, "Filter for >" + filter + "< is unkown.");
            Logger.addMessage(msg);
        }
        return f;
    }
}
