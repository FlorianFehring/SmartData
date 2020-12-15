package de.fhbielefeld.smartdata.dynrecords.filter;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.smartdata.dyncollection.DynCollection;

/**
 *
 * @author Florian Fehring
 */
public class FilterParser {

    public static Filter parse(String filter, DynCollection table) throws FilterException {
        Filter f = null;
        String[] parts = filter.split(",");
        if(parts.length < 2) {
            throw new FilterException("Filter is not valid.");
        }
        String filtername = parts[1];
        switch (filtername) {
            case "cs":
            case "ncs":
                f = new ContainFilter(table);
		f.parse(filter);
                break;
            case "sw":
            case "nsw":
                f = new StartsWithFilter(table);
		f.parse(filter);
                break;
            case "ew":
            case "new":
                f = new EndsWithFilter(table);
		f.parse(filter);
                break;
            case "eq":
            case "neq":
                f = new EqualsFilter(table);
		f.parse(filter);
                break;
            case "lt":
            case "nlt":
                f = new LowerThanFilter(table);
		f.parse(filter);
                break;
            case "le":
            case "nle":
                f = new LowerOrEqualFilter(table);
		f.parse(filter);
                break;
            case "ge":
            case "nge":
                f = new GreaterOrEqualFilter(table);
		f.parse(filter);
                break;
            case "gt":
            case "ngt":
                f = new GreaterThanFilter(table);
		f.parse(filter);
                break;
            case "bt":
            case "nbt":
                f = new BetweenFilter(table);
		f.parse(filter);
                break;
            case "in":
            case "nin":
                f = new InFilter(table);
		f.parse(filter);
                break;
            case "is":
            case "nis":
                f = new IsNullFilter(table);
		f.parse(filter);
                break;
            case "sir":
                f = new RadiusFilter(table);
		f.parse(filter);
                break;
            case "sib":
                f = new BoundingBoxFilter(table);
		f.parse(filter);
                break;
            case "sco":
                f = new ContainsGeoFilter(table);
                f.parse(filter);
                break;
            case "scr":
                f = new CrossesGeoFilter(table);
                f.parse(filter);
                break;
            case "sdi":
                f = new DisjointGeoFilter(table);
                f.parse(filter);
                break;
            case "seq":
                f = new EqualsGeoFilter(table);
                f.parse(filter);
                break;
            case "sin":
                f = new IntersectsGeoFilter(table);
                f.parse(filter);
                break;
            case "sov":
                f = new OverlapsGeoFilter(table);
                f.parse(filter);
                break;
            default:
                Message msg = new Message("SmartData", MessageLevel.ERROR, "Filter >"+ filtername +"< for >" + filter + "< is unkown.");
		Logger.addMessage(msg);
            }
        return f;
    }
}