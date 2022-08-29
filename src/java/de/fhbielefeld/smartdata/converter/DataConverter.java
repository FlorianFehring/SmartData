package de.fhbielefeld.smartdata.converter;

import de.fhbielefeld.smartdata.exceptions.DynException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Class for converting objects into a specialised object.
 * 
 * @author ffehring
 */
public class DataConverter {

    public static Double objectToDouble(Object value) throws DynException {
        if (value instanceof Double) {
            return (Double) value;
        } else {
            try {
                String stringValue = (String) value;
                if (stringValue.isEmpty() || stringValue.equalsIgnoreCase("NAN")) {
                    throw new DynException("Error parsing value >" + value + "< to double.");
                } else if (stringValue.contains(",")) {
                    NumberFormat format = NumberFormat.getInstance(Locale.GERMANY);
                    Number number = format.parse(stringValue);
                    return number.doubleValue();
                } else {
                    return Double.parseDouble(stringValue);
                }
            } catch (NumberFormatException | ParseException ex) {
                DynException pex = new DynException("Error parsing value >" + value + "<: " + ex.getLocalizedMessage());
                pex.addSuppressed(ex);
                throw pex;
            }
        }
    }

    public static Integer objectToInteger(Object value) throws DynException {
        if (value instanceof Integer) {
            return (Integer) value;
        } else {
            try {
                String stringValue = (String) value;
                if (stringValue.isEmpty() || stringValue.equalsIgnoreCase("NAN")) {
                    throw new DynException("Error parsing value >" + value + "< to integer.");
                } else {
                    return Integer.parseInt(stringValue);
                }
            } catch (NumberFormatException ex) {
                DynException pex = new DynException("Error parsing value >" + value + "<: " + ex.getLocalizedMessage());
                pex.addSuppressed(ex);
                throw pex;
            }
        }
    }

    public static Short objectToShort(Object value) throws DynException {
        if (value instanceof Short) {
            return (Short) value;
        } else if (value instanceof Integer) {
            return ((Integer) value).shortValue();
        } else {
            try {
                String stringValue = (String) value;
                if (stringValue.isEmpty() || stringValue.equalsIgnoreCase("NAN")) {
                    throw new DynException("Error parsing value >" + value + "< to short.");
                } else {
                    return Short.parseShort(stringValue);
                }
            } catch (NumberFormatException ex) {
                DynException pex = new DynException("Error parsing value >" + value + "<: " + ex.getLocalizedMessage());
                pex.addSuppressed(ex);
                throw pex;
            }
        }
    }

    public static Long objectToLong(Object value) throws DynException {
        if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof Integer) {
            return ((Integer) value).longValue();
        } else {
            try {
                String stringValue = (String) value;
                if (stringValue.isEmpty() || stringValue.equalsIgnoreCase("NAN")) {
                    throw new DynException("Error parsing value >" + value + "< to short.");
                } else {
                    return Long.parseLong(stringValue);
                }
            } catch (NumberFormatException ex) {
                DynException pex = new DynException("Error parsing value >" + value + "<: " + ex.getLocalizedMessage());
                pex.addSuppressed(ex);
                throw pex;
            }
        }
    }

    public static Boolean objectToBoolean(Object value) throws DynException {
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else {
            try {
                return Boolean.parseBoolean((String) value);
            } catch (NumberFormatException ex) {
                DynException pex = new DynException("Error parsing value >" + value + "<: " + ex.getLocalizedMessage());
                pex.addSuppressed(ex);
                throw pex;
            }
        }
    }

    public static LocalDate objectToLocalDate(Object value) throws DynException {
        if (value instanceof LocalDate) {
            return (LocalDate) value;
        } else if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value).toLocalDate();
        } else if (value instanceof java.util.Date) {
            return ((java.util.Date) value).toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate();
        } else {
            String datetimestring = (String) value;
            List<DynException> exs = new ArrayList<>();
            // Try parsing as long and interpret as timestamp
            try {
                Long days = Long.parseLong((String) value);
                return LocalDate.of(1899, 12, 30).plusDays(days);
            } catch (NumberFormatException ex) {
                DynException pex = new DynException("Can not parse value >" + value + "< as date: " + ex.getLocalizedMessage());
                pex.addSuppressed(ex);
                exs.add(pex);
            }
            
            // Try parse after ISO8601
            try {
                DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE;
                LocalDate date = LocalDate.parse(datetimestring, dtf);
                return date;
            } catch (DateTimeParseException ex) {
                DynException pex = new DynException("Could not parse >"
                        + datetimestring
                        + "< as LocalDateTime after ISO pattern (2011-12-30)"
                        + ex.getLocalizedMessage());
                pex.addSuppressed(ex);
                exs.add(pex);
            }
            
            DynException nex = new DynException("Could not parse date");
            for(Exception curEx : exs) {
                nex.addSuppressed(curEx);
                System.out.println(curEx.getLocalizedMessage());
            }
            throw nex;
        }
    }

    /**
     * Converts the contents of an object into a local date time if possible.
     * 
     * @param value Object, thats string representation can be converted to a localdetetime
     * @return LocalDateTime interpretation of the content
     * @throws DynException 
     */
    public static LocalDateTime objectToLocalDateTime(Object value) throws DynException {
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        } else {
            List<DynException> exs = new ArrayList<>();
            String datetimestring = (String) value;
            // correct format 2022-08-27 to 2022-08-27 00:00:00
            if(datetimestring != null && datetimestring.contains("-") && datetimestring.length() == 10) {
                datetimestring = datetimestring + " 00:00:00";
            }
            
            // Try parse string to long
            try {
                Long timestamp = Long.parseLong(datetimestring);
                LocalDateTime datetime = LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), TimeZone
                        .getDefault().toZoneId());
                return datetime;
            } catch (NumberFormatException | DateTimeParseException ex) {
                DynException pex = new DynException("Could not interpret >"
                        + datetimestring + "< as timestamp. " + ex.getLocalizedMessage());
                pex.addSuppressed(ex);
                exs.add(pex);
            }

            // Try parse as iso datetime
            try {
                DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE_TIME;
                LocalDateTime datetime = LocalDateTime.parse(datetimestring, dtf);
                return datetime;
            } catch (DateTimeParseException ex) {
                DynException pex = new DynException("Could not parse >"
                        + datetimestring
                        + "< as LocalDateTime after ISO pattern (2011-12-30T10:15:30)"
                        + ex.getLocalizedMessage());
                pex.addSuppressed(ex);
                exs.add(pex);
            }

            // Try parse as german datetime
            try {
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm[:ss][.SSSSSSSSS][.SSSSSSSS][.SSSSSSS][.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]");
                LocalDateTime datetime = LocalDateTime.parse(datetimestring, dtf);
                return datetime;
            } catch (DateTimeParseException ex) {
                DynException pex = new DynException(
                        "Could not parse >" + datetimestring
                        + "< as LocalDateTime after ISO-T pattern (2011-12-30 10:15:30.123)"
                        + ex.getLocalizedMessage());
                pex.addSuppressed(ex);
                exs.add(pex);
            }            

            // Try parse as german datetime
            try {
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm[:ss][.SSSSSSSSS][.SSSSSSSS][.SSSSSSS][.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]");
                LocalDateTime datetime = LocalDateTime.parse(datetimestring, dtf);
                return datetime;
            } catch (DateTimeParseException ex) {
                DynException pex = new DynException(
                        "Could not parse >" + datetimestring 
                                + "< as LocalDateTime after DE pattern (30.12.2011 10:15:30)"
                        + ex.getLocalizedMessage());
                pex.addSuppressed(ex);
                exs.add(pex);
            }
            
            // Try parse as file name date
            // 2022 07 08 14 11 18
            try {
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy MM dd HH mm ss");
                LocalDateTime datetime = LocalDateTime.parse(datetimestring, dtf);
                return datetime;
            } catch (DateTimeParseException ex) {
                DynException pex = new DynException(
                        "Could not parse >" + datetimestring 
                                + "< as LocalDateTime after DE pattern (30.12.2011 10:15:30)"
                        + ex.getLocalizedMessage());
                pex.addSuppressed(ex);
                exs.add(pex);
            }

            DynException nex = new DynException("Could not parse datetime");
            nex.printStackTrace();
            for(Exception curEx : exs) {
                nex.addSuppressed(curEx);
                System.out.println(curEx.getLocalizedMessage());
            }
            throw nex;
        }
    }
}
