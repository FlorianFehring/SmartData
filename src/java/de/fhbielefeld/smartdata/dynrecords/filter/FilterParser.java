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
            throw new FilterException("Filter has less then 2 parts.");
        }
        String filtername = parts[1];
        switch (filtername) {
            case "cs":
            case "ncs":
                if(parts.length < 3) {
                    throw new FilterException("n*cs Filter >"+filter+"< is not valid.");
                } else {
                    f = new ContainFilter(table);
                    f.parse(filter);
                }
                break;
            case "sw":
            case "nsw":
                if(parts.length < 3) {
                    throw new FilterException("n*sw Filter >"+filter+"< is not valid.");
                } else {
                    f = new StartsWithFilter(table);
                    f.parse(filter);
                }
                break;
            case "ew":
            case "new":
                if(parts.length < 3) {
                    throw new FilterException("n*ew Filter >"+filter+"< is not valid.");
                } else {
                    f = new EndsWithFilter(table);
                    f.parse(filter);
                }
                break;
            case "eq":
            case "neq":
                if(parts.length < 3) {
                    throw new FilterException("n*eq Filter >"+filter+"< is not valid.");
                } else {
                    f = new EqualsFilter(table);
                    f.parse(filter);
                }
                break;
            case "lt":
            case "nlt":
                if(parts.length < 3) {
                    throw new FilterException("n*lt Filter >"+filter+"< is not valid.");
                } else {
                    f = new LowerThanFilter(table);
                    f.parse(filter);
                }
                break;
            case "le":
            case "nle":
                if(parts.length < 3) {
                    throw new FilterException("n*le Filter >"+filter+"< is not valid.");
                } else {
                    f = new LowerOrEqualFilter(table);
                    f.parse(filter);
                }
                break;
            case "ge":
            case "nge":
                if(parts.length < 3) {
                    throw new FilterException("n*ge Filter >"+filter+"< is not valid.");
                } else {
                    f = new GreaterOrEqualFilter(table);
                    f.parse(filter);
                }
                break;
            case "gt":
            case "ngt":
                if(parts.length < 3) {
                    throw new FilterException("n*gt Filter >"+filter+"< is not valid.");
                } else {
                    f = new GreaterThanFilter(table);
                    f.parse(filter);
                }
                break;
            case "bt":
            case "nbt":
                if(parts.length < 4) {
                    throw new FilterException("n*bt Filter >"+filter+"< is not valid.");
                } else {
                    f = new BetweenFilter(table);
                    f.parse(filter);
                }
                break;
            case "in":
            case "nin":
                if(parts.length < 3) {
                    throw new FilterException("n*in Filter >"+filter+"< is not valid.");
                } else {
                    f = new InFilter(table);
                    f.parse(filter);
                }
                break;
            case "is":
            case "nis":
                f = new IsNullFilter(table);
		f.parse(filter);
                break;
            case "sir":
                if(parts.length < 6) {
                    throw new FilterException("n*is Filter >"+filter+"< is not valid.");
                } else {
                    f = new RadiusFilter(table);
                    f.parse(filter);
                }
                break;
            case "sib":
                if(parts.length < 8) {
                    throw new FilterException("sib Filter >"+filter+"< is not valid.");
                } else {
                    f = new BoundingBoxFilter(table);
                    f.parse(filter);
                }
                break;
            case "sco":
                if(parts.length < 3) {
                    throw new FilterException("sco Filter >"+filter+"< is not valid.");
                } else {
                    f = new ContainsGeoFilter(table);
                    f.parse(filter);
                }
                break;
            case "scr":
                if(parts.length < 3) {
                    throw new FilterException("scr Filter >"+filter+"< is not valid.");
                } else {
                    f = new CrossesGeoFilter(table);
                    f.parse(filter);
                }
                break;
            case "sdi":
                if(parts.length < 3) {
                    throw new FilterException("sdi Filter >"+filter+"< is not valid.");
                } else {
                    f = new DisjointGeoFilter(table);
                    f.parse(filter);
                }
                break;
            case "seq":
                if(parts.length < 3) {
                    throw new FilterException("seq Filter >"+filter+"< is not valid.");
                } else {
                    f = new EqualsGeoFilter(table);
                    f.parse(filter);
                }
                break;
            case "sin":
                if(parts.length < 3) {
                    throw new FilterException("sin Filter >"+filter+"< is not valid.");
                } else {
                    f = new IntersectsGeoFilter(table);
                    f.parse(filter);
                }
                break;
            case "sov":
                if(parts.length < 3) {
                    throw new FilterException("sov Filter >"+filter+"< is not valid.");
                } else {
                    f = new OverlapsGeoFilter(table);
                    f.parse(filter);
                }
                break;
            case "sto":
                if(parts.length < 3) {
                    throw new FilterException("sto Filter >"+filter+"< is not valid.");
                } else {
                    f = new TouchesGeoFilter(table);
                    f.parse(filter);
                }
                break;
            case "swi":
                if(parts.length < 3) {
                    throw new FilterException("swi Filter >"+filter+"< is not valid.");
                } else {
                    f = new WithinGeoFilter(table);
                    f.parse(filter);
                }
                break;
            case "sic":
                f = new IsClosedGeoFilter(table);
                f.parse(filter);
                break;
            case "sis":
                f = new IsSimpleGeoFilter(table);
                f.parse(filter);
                break;
            case "siv":
                f = new IsValidGeoFilter(table);
                f.parse(filter);
                break;
            default:
                Message msg = new Message("SmartData", MessageLevel.ERROR, "Filter >"+ filtername +"< for >" + filter + "< is unkown.");
		Logger.addMessage(msg);
            }
        return f;
    }
}