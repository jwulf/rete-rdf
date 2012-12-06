/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package rdf;

/**
 *
 * @author David Monks
 * @version 1.5
 */
import java.util.TimeZone;
import java.sql.Time;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

public class Triple {
    
    public static final String OPEN_CHAR = "[";
    public static final String CLOSE_CHAR = "]";
    public static final String SEPARATOR = ",";
    
    private Pair<Object,RDFType> subject,predicate,object;
    
    /**
     * Converts a string to a list of triples.
     * @param String input - a string containing a list of one or more triples in N-triples format
     * @return List<Triple> triples - the list of Triples from the input string as Triple objects
     * @throws RDFTypeException 
     */
    public static List<Triple> listFromString(String input) throws RDFTypeException{
        int base = 0;
        int offset = 0;
        List<Triple> list = new ArrayList<Triple>();
        
        input = input.trim();
        String exceptionMessage = "";
        while (offset < input.length()){
            int nestCount = 0;
            //Find the next whole N3 triple (between a set of OPWN_CHAR and CLOSE_CHAR).
            do {
                if (input.charAt(offset) == OPEN_CHAR.charAt(0)){
                    nestCount++;
                }else if (input.charAt(offset) == '"'){
                    do {
                        offset = input.indexOf("\"", offset + 1);
                    } while (input.charAt(offset - 1) == '\\');
                }else if (input.charAt(offset) == CLOSE_CHAR.charAt(0)){
                    nestCount--;
                }
                offset++;
            } while (nestCount > 0);
            //convert the String triple to a Triple object and add it to the list of triples.
            try{
                list.add(tripleFromString(input.substring(base, offset)));
            }catch(RDFTypeException e){
                exceptionMessage += e.getMessage()+"\n";
            }catch(RDFPartException e){
                exceptionMessage += e.getMessage()+"\n";
            }
            //progress to the next part of the string
            base = offset;
        }
        if (exceptionMessage.equals(""))
            throw new RDFTypeException("*** Plain triple errors ***\n"+exceptionMessage,list);
        return list;
    }
    
    /**
     * Extracts a single Triple from a string.
     * @param String input - a string representation of a triple
     * @return Triple triple - The object representing the triple in the input string
     * @throws RDFPartException
     * @throws RDFTypeException 
     */
    public static Triple tripleFromString(String input) throws RDFPartException,RDFTypeException{
        String[] tripleParts = new String[3];
        //Trim away the character that opens the triple, along with the any space between that character and the first element.
        if (input.contains(Triple.OPEN_CHAR)){
            input = input.substring(input.indexOf(Triple.OPEN_CHAR)+1).trim();
        }
        int offset = 0;
        //extract each part of the triple
        for (int i = 0; i < tripleParts.length; i++){
            tripleParts[i] = matchAndConsumeDataItem(input,offset);
            offset += tripleParts[i].length() + 1;
        }
        //create the triple object from the string forms of its parts, and return it
        return new Triple(tripleParts[0].trim(),
                          tripleParts[1].trim(),
                          tripleParts[2].trim());
    }
    
    /**
     * Extract a piece of data from the input string.
     * @param String input - the string from which the next piece of data is to be extracted
     * @param int offset - to offset from the start of the string after which the data should be extracted
     * @return String dataItem - the piece of data extracted.
     * @throws RDFPartException 
     */
    public static String matchAndConsumeDataItem(String input, int offset) throws RDFPartException{
        int base = offset;
        if (input.length() <= offset) throw new RDFPartException("Empty Data Item.");
        //if the string starts with a quote mark, advance the offset until a second, unescaped quote mark is found.
        if (input.charAt(offset) == '"'){
            do {
                offset = input.indexOf("\"", offset + 1);
            } while (input.charAt(offset - 1) == '\\');
        }
        //glob the remainder of the input up until a separation character into the piece of data, if a separation character exists beyond the offset.
        if (input.indexOf(SEPARATOR, offset+1) > -1){
            if (input.indexOf(Triple.CLOSE_CHAR, offset+1) > -1 && input.indexOf(Triple.CLOSE_CHAR, offset+1) < input.indexOf(SEPARATOR, offset+1)){
                return input.substring(base,input.indexOf(Triple.CLOSE_CHAR,offset+1));
            }
            //return the data
            return input.substring(base,input.indexOf(SEPARATOR,offset+1));
        }
        //glob the remainder of the input up until a triple-closing character, if one exists.
        if (input.indexOf(Triple.CLOSE_CHAR, offset+1) > -1){
            //return the data
            return input.substring(base,input.indexOf(Triple.CLOSE_CHAR,offset+1));
        }
        //return the data (the rest of the string).
        return input.substring(base);
    }
    
    /**
     * Creates a new triple object, setting the subject, predicate and object of the triple to the arguments s, p and o respectively.
     * @param String s
     * @param String p
     * @param String o 
     */
    public Triple(String s,
                  String p,
                  String o) throws RDFTypeException{
        Pair<Object,RDFType> pair = sanitise(s);
        subject = pair;
        
        pair = sanitise(p);
        predicate = pair;
        
        pair = sanitise(o);
        object = pair;
    }
    /**
     * Creates a new triple object, setting the subject, predicate and object of the triple to the arguments sub, pred and ob respectively.
     * @param Pair<Object,RDFType> sub
     * @param Pair<Object,RDFType> pred
     * @param Pair<Object,RDFType> ob
     */
    public Triple(Pair<Object,RDFType> sub,
                  Pair<Object,RDFType> pred,
                  Pair<Object,RDFType> ob){
        subject = sub;
        predicate = pred;
        object = ob;
    }
    
    /**
     * Takes an RDF element string and converts it to the appropriate java type, keeping a record of their original types:
     * *    XSD Integers are converted to int.
     * *    XSD Decimals are converted to double.
     * *    XSD Booleans are converted to boolean.
     * *    XSD Strings are unwrapped from their typed literal wrappers as String objects.
     * *    XSD Dates and DateTimes are converted into long, representing milliseconds from the Unix epoch, UTC.
     * *    XSD Times are converted into long, representing milliseconds from the start of the day (00:00:00 UTC, accepting times that start before this time due to time zone as negative times).
     * *    XSD Durations are converted into long, representing milliseconds from the start of the time period (accepts negative durations).
     * 
     * *    Plain text integers and doubles are converted to int or double respectively.
     * *    URIs and IRIs are stripped of their enclosing angle brackets and stored as strings.
     * *    Blank nodes are stored as strings.
     * @param String item
     * @return Pair<Object,RDFType>
     * @throws RDFTypeException 
     */
    private static Pair<Object,RDFType> sanitise(String item) throws RDFTypeException{
        String[] parts = item.split("\"");
        Object returnable;
        RDFType returnableType;
        switch (parts.length){
            case 3:
                if (parts[2].equals("")){
                    returnableType = RDFType.PLAIN_LITERAL;
                    returnable = parts[1];
                }else{
                    parts[2] = parts[2].trim();
                    switch (parts[2].charAt(0)){
                        case '@':
                            returnableType = RDFType.PLAIN_LITERAL;
                            returnable = parts[1];
                            break;
                        case '^':
                            String[] temp;
                            String[] sDataType = parts[2].split("#");
                            if (sDataType[0].equals("^^<http://www.w3.org/2001/XMLSchema"))
                                switch (sDataType[1].charAt(1)){
                                    case 'n':
                                        returnableType = RDFType.INTEGER;
                                        returnable = new Integer(parts[1]);
                                        break;
                                    case 'o':
                                        returnableType = RDFType.BOOLEAN;
                                        returnable = new Boolean(parts[1].compareToIgnoreCase("true") == 0);
                                        break;
                                    case 'e':
                                        returnableType = RDFType.DECIMAL;
                                        returnable = new Double(parts[1]);
                                        break;
                                    case 'a':
                                        if (sDataType[1].length() > 4){
                                            returnableType = RDFType.DATETIME;
                                            String[] datetime = parts[1].split("T");
                                            String timezone = "GMT";
                                            if (datetime[1].contains("Z")){
                                                datetime[1] = datetime[1].split("Z")[0];
                                                timezone += "+00:00";
                                            }else if (datetime[1].contains("-")){
                                                datetime[1] = (temp = datetime[1].split("-"))[0];
                                                timezone += "-"+temp[1];
                                            }else if (datetime[1].contains("+")){
                                                datetime[1] = (temp = datetime[1].split("+"))[0];
                                                timezone += "+"+temp[1];
                                            }else{
                                                timezone += "+00:00";
                                            }
                                            returnable = new Long(Time.valueOf(datetime[1]).getTime()
                                                      +Date.valueOf(datetime[0]).getTime()
                                                      +TimeZone.getTimeZone(timezone).getRawOffset());
                                        }else{
                                            returnableType = RDFType.DATE;
                                            String[] date;
                                            String timezone = "GMT";
                                            if (parts[1].contains("Z")){
                                                parts[1] = parts[1].split("Z")[0];
                                                timezone += "+00:00";
                                            }else if ((date = parts[1].split("-")).length > 3){
                                                timezone += "-"+date[3];
                                            }else if (parts[1].contains("+")){
                                                parts[1] = (temp = parts[1].split("+"))[0];
                                                timezone += "+"+temp[1];
                                            }else{
                                                timezone += "+00:00";
                                            }
                                            returnable = new Long(Date.valueOf(parts[1]).getTime()
                                                      +TimeZone.getTimeZone(timezone).getRawOffset());
                                        }
                                        break;
                                    case 'i':
                                        returnableType = RDFType.TIME;
                                        String timezone = "GMT";
                                        if (parts[1].contains("Z")){
                                            parts[1] = parts[1].split("Z")[0];
                                            timezone += "+00:00";
                                        }else if (parts[1].contains("-")){
                                            parts[1] = (temp = parts[1].split("-"))[0];
                                            timezone += "-"+temp[1];
                                        }else if (parts[1].contains("+")){
                                            parts[1] = (temp = parts[1].split("+"))[0];
                                            timezone += "+"+temp[1];
                                        }else{
                                            timezone += "+00:00";
                                        }
                                        returnable = new Long(Time.valueOf(parts[1]).getTime()
                                                  +TimeZone.getTimeZone(timezone).getRawOffset());
                                        break;
                                    case 'u':
                                        returnableType = RDFType.DURATION;
                                        temp = parts[1].split("P");
                                        boolean negative = temp[0].equals("-");
                                        int Y = 0;
                                        int M = 0;
                                        int D = 0;
                                        int h = 0;
                                        int m = 0;
                                        int s = 0;
                                        temp = temp[1].split("T");
                                        String date = temp[0];
                                        String time = temp[1];
                                        temp = date.split("Y");
                                        if (temp.length > 1){
                                            date = temp[1];
                                            Y = Integer.parseInt(temp[0]);
                                        }
                                        temp = date.split("M");
                                        if (temp.length > 1){
                                            date = temp[1];
                                            M = Integer.parseInt(temp[0]);
                                        }
                                        temp = date.split("D");
                                        if (temp.length > 1)
                                            D = Integer.parseInt(temp[0]);
                                        temp = time.split("H");
                                        if (temp.length > 1){
                                            time = temp[1];
                                            h = Integer.parseInt(temp[0]);
                                        }
                                        temp = time.split("M");
                                        if (temp.length > 1){
                                            time = temp[1];
                                            m = Integer.parseInt(temp[0]);
                                        }
                                        temp = time.split("S");
                                        if (temp.length > 1)
                                            s = Integer.parseInt(temp[0]);
                                        Calendar calendar = new GregorianCalendar();
                                        calendar.set(Y+1970,M,D+1,h,m,s);
                                        returnable = new Long(negative ? -calendar.getTimeInMillis() : calendar.getTimeInMillis());
                                        break;
                                    case 't':
                                        returnableType = RDFType.STRING;
                                        returnable = parts[1];
                                        break;
                                    default:
                                        returnableType = RDFType.TYPED_LITERAL;
                                        returnable = "\""+parts[1]+"\""+parts[2];
                                }
                            else{
                                returnableType = RDFType.TYPED_LITERAL;
                                returnable = "\""+parts[1]+"\""+parts[2];
                            }
                            break;
                        default:
                            throw new RDFTypeException("Unrecognised literal descriptor: "+parts[2]);
                    }
                } 
                break;
            case 2:
                try{
                    String[] datetime = parts[1].split("T");
                    String timezone = "GMT";
                    if (datetime[1].contains("Z")){
                        timezone += "+00:00";
                        datetime[1] = datetime[1].substring(0,datetime[1].indexOf("Z"));
                    }else if (datetime[1].contains("-")){
                        timezone += datetime[1].substring(datetime[1].indexOf("-"));
                        datetime[1] = datetime[1].substring(0,datetime[1].indexOf("-"));
                    }else if (datetime[1].contains("+")){
                        timezone += datetime[1].substring(datetime[1].indexOf("+"));
                        datetime[1] = datetime[1].substring(0,datetime[1].indexOf("+"));
                    }else{
                        timezone += "+00:00";
                    }
                    return new Pair<Object,RDFType>(new Long(Time.valueOf(datetime[1]).getTime()
                                                        +Date.valueOf(datetime[0]).getTime()
                                                        +TimeZone.getTimeZone(timezone).getRawOffset()),
                                                    RDFType.DATETIME);
                }catch(Exception ex){}
                try{
                    String timezone = "GMT";
                    String[] temp;
                    if (parts[1].contains("Z")){
                        timezone += "+00:00";
                        parts[1] = parts[1].substring(0,parts[1].indexOf("Z"));
                    }else if ((temp = parts[1].split("-")).length > 3){
                        parts[1] = temp[0]+"-"+temp[1]+"-"+temp[2];
                        timezone += "-"+temp[3];
                    }else if (parts[1].contains("+")){
                        timezone += parts[1].substring(parts[1].indexOf("+"));
                        parts[1] = parts[1].substring(0,parts[1].indexOf("+"));
                    }else{
                        timezone += "+00:00";
                    }
                    return new Pair<Object,RDFType>(new Long(Date.valueOf(parts[0]).getTime()
                                                        +TimeZone.getTimeZone(timezone).getRawOffset()),
                                                    RDFType.DATE);
                }catch(Exception ex){}
                try{
                    double number = Double.parseDouble(parts[1]);
                    if (Math.floor(number) < number){
                        return new Pair<Object,RDFType>(new Double(number),RDFType.DECIMAL);
                    }else{
                        return new Pair<Object,RDFType>(new Integer((new Double(number)).intValue()),RDFType.INTEGER);
                    }
                }catch (NumberFormatException ex){}
                returnableType = RDFType.PLAIN_LITERAL;
                returnable = parts[1];
                break;
            case 1:
                switch (item.charAt(0)){
                    case '-':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                    case '0':
                        try{
                            double number = Double.parseDouble(item);
                            if (Math.floor(number) < number){
                                returnable = new Double(number);
                                returnableType = RDFType.DECIMAL;
                            }else{
                                String num = Double.toString(number);
                                returnable = new Integer(num.substring(0,num.length()-2));
                                returnableType = RDFType.INTEGER;
                            }
                        }catch (NumberFormatException ex){
                            returnable = item;
                            returnableType = RDFType.BLANK;
                        }
                        break;
                    case '<':
                        item = item.substring(1,item.length()-1);
                    case 'h':
                        if (item.contains("://")){
                            returnable = item;
                            returnableType = RDFType.URI;
                            break;
                        }else{
                            returnable = item;
                            returnableType = RDFType.BLANK;
                        }
                    case '_':
                        item = item.substring(2);
                    default:
                        returnable = item;
                        returnableType = RDFType.BLANK;
//                        throw new RDFTypeException("Unrecognised RDF formatting: "+item);
                }
                break;
            default:
                throw new RDFTypeException("Unrecognised RDF formatting: "+item);
        }
        return new Pair<Object,RDFType>(returnable,returnableType);
    }
    /**
     * Wrapper for sanitise method, allowing it to be used by sub-classes but not overriden (though they may still edit this class so that it does not call sanitise, but that is their perogative).
     * 
     * sanitise(String item):
     * Takes an RDF element string and converts it to the appropriate java type, keeping a record of their original types:
     * *    XSD Integers are converted to int.
     * *    XSD Decimals are converted to double.
     * *    XSD Booleans are converted to boolean.
     * *    XSD Strings are unwrapped from their typed literal wrappers as String objects.
     * *    XSD Dates and DateTimes are converted into long, representing milliseconds from the Unix epoch, UTC.
     * *    XSD Times are converted into long, representing milliseconds from the start of the day (00:00:00 UTC, accepting times that start before this time due to time zone as negative times).
     * *    XSD Durations are converted into long, representing milliseconds from the start of the time period (accepts negative durations).
     * 
     * *    Plain text integers and doubles are converted to int or double respectively.
     * *    URIs and IRIs are stripped of their enclosing angle brackets and stored as strings.
     * *    Blank nodes are stored as strings.
     * @param String item
     * @return Pair<Object,RDFType>
     * @throws RDFTypeException 
     */
    protected static Pair<Object,RDFType> convertType(String item) throws RDFTypeException{
        return sanitise(item);
    }
    public static Object convertToJava(String item) throws RDFTypeException{
        return sanitise(item.trim()).item1;
    }
    
    /**
     * Returns the Subject of the triple as a Pair of the Subject in its Java form and its RDF Type.
     * @return Pair<Object,RDFType> subject
     */
    public Pair<Object,RDFType> getSubject(){
        return subject;
    }
    /**
     * Returns the Predicate of the triple as a Pair of the Subject in its Java form and its RDF Type.
     * @return Pair<Object,RDFType> predicate
     */
    public Pair<Object,RDFType> getPredicate(){
        return predicate;
    }
    /**
     * Returns the Object of the triple as a Pair of the Subject in its Java form and its RDF Type.
     * @return Pair<Object,RDFType> object
     */
    public Pair<Object,RDFType> getObject(){
        return object;
    }
    /**
     * Returns the part of the triple indicated by part as an Object.
     * @param TuplePart part
     * @return Object item
     */
    public Object getPart(TuplePart p) throws RDFPartException{
        switch (p){
            case SUBJECT:
                return subject.get1();
            case PREDICATE:
                return predicate.get1();
            case OBJECT:
                return object.get1();
            default:
                throw new RDFPartException("Tuple does not contain this data: "+p.toString());
        }
    }
    /**
     * Returns the type of the part of the triple indicated by part.
     * @param TuplePart part
     * @return RDFType type
     */
    public RDFType getPartType(TuplePart p) throws RDFPartException{
        switch (p){
            case SUBJECT:
                return subject.get2();
            case PREDICATE:
                return predicate.get2();
            case OBJECT:
                return object.get2();
            default:
                throw new RDFPartException("Tuple does not contain this data: "+p.toString());
        }
    }
    
    /**
     * Returns the content of the triple in RDF, having converted all parts to their equivalent RDF format.
     * @return String RDF
     */
    public String toRDF() throws RDFPartException, RDFTypeException{
        return Triple.OPEN_CHAR+partToRDF(TuplePart.SUBJECT)
              +Triple.SEPARATOR+partToRDF(TuplePart.PREDICATE)
              +Triple.SEPARATOR+partToRDF(TuplePart.OBJECT)
              +Triple.CLOSE_CHAR;
    }
    /**
     * Returns the part of the tuple specified by TuplePart part as a string incorporating an rdf type.
     * @param TuplePart p
     * @return String rdf
     */
    protected String partToRDF(TuplePart part) throws RDFPartException, RDFTypeException{
        switch (getPartType(part)){
            case URI:
                return /*"<"+*/(String)getPart(part)/*+">"*/;
            case INTEGER:
                return "\""+((Integer)getPart(part)).toString()+"\"^^<http://www.w3.org/2001/XMLSchema#integer>";
            case DECIMAL:
                return "\""+((Double)getPart(part)).toString()+"\"^^<http://www.w3.org/2001/XMLSchema#decimal>";
            case STRING:
                return "\""+(String)getPart(part)+"\"^^<http://www.w3.org/2001/XMLSchema#string>";
            case DATE:
                return "\""+(new Date(((Long)getPart(part)).longValue())).toString()+"\"^^<http://www.w3.org/2001/XMLSchema#date>";
            case TIME:
                return "\""+(new Time(((Long)getPart(part)).longValue())).toString()+"\"^^<http://www.w3.org/2001/XMLSchema#time>";
            case DATETIME:
                return "\""+(new Date(((Long)getPart(part)).longValue())).toString()+"T"+(new Time(((Long)getPart(part)).longValue())).toString()+"\"^^<http://www.w3.org/2001/XMLSchema#datetime>";
            case DURATION:
                Calendar calendar = new GregorianCalendar();
                calendar.setTimeInMillis(Math.abs(((Long)getPart(part)).longValue()));
                return (((Long)getPart(part)).longValue() < 0 ? "\"-P" : "\"P")
                        +(calendar.get(Calendar.YEAR) > 0 ? Integer.toString(calendar.get(Calendar.YEAR)-1970)+"Y" : "")
                        +(calendar.get(Calendar.MONTH) > 0 ? Integer.toString(calendar.get(Calendar.MONTH))+"M" : "")
                        +(calendar.get(Calendar.DATE) > 0 ? Integer.toString(calendar.get(Calendar.DATE))+"DT" : "T")
                        +(calendar.get(Calendar.HOUR) > 0 ? Integer.toString(calendar.get(Calendar.HOUR))+"H" : "")
                        +(calendar.get(Calendar.MINUTE) > 0 ? Integer.toString(calendar.get(Calendar.MINUTE))+"M" : "")
                        +(calendar.get(Calendar.SECOND) > 0 ? Integer.toString(calendar.get(Calendar.SECOND))+"S" : "")
                        +"\"^^<http://www.w3.org/2001/XMLSchema#duration>";
            case BOOLEAN:
                return (((Boolean)getPart(part)).booleanValue() ? "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>" : "\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>");
            case PLAIN_LITERAL:
                return "\""+(String)getPart(part)+"\"";
            case BLANK:
                return "_:"+(String)getPart(part);
            case TYPED_LITERAL:
                return (String)getPart(part);
            default:
                throw new RDFTypeException("Unrecognised type stored.  SHOULD NOT BE POSSIBLE.");
        }
    }
    /**
     * Returns the content of the triple in XML RDF, having converted all parts to their equivalent XML RDF format.
     * @return String XMLRDF
     */
    public String toXMLRDF() throws RDFPartException, RDFTypeException{
        //TODO
        throw new UnsupportedOperationException("TODO Triple-to-RDF conversion.");
    }
    /**
     * Returns the part of the tuple specified by TuplePart part as a string incorporating an rdf type.
     * @param TuplePart p
     * @return String rdf
     */
    protected String partToXMLRDF(TuplePart part) throws RDFPartException, RDFTypeException{
        //TODO
        throw new UnsupportedOperationException("TODO Triple-to-RDF conversion.");
    }
    
    /**
     * If the non-time annotated parts of the triple are equal, make the current triple represent the longevity of itself plus the triple it is being combined with.
     * Return true after a successful combine, false otherwise.
     * @param Triple other - the triple to be combined with this one
     * @return boolean combined - whether the triples were combined
     */
    public boolean combine(Triple other){
        return equals(other);
    }
    
    @Override
    public boolean equals(Object o){
        if (o instanceof Triple){
            Triple other = (Triple)o;
            return subject.equals(other.subject) 
                && predicate.equals(other.predicate) 
                && object.equals(other.object);
        }
        return false;
    }

    //Nested Classes
    
    public static class Pair <P1,P2>{

        private P1 item1;
        private P2 item2;

        public Pair(P1 i1, P2 i2){
            item1 = i1;
            item2 = i2;
        }

        public P1 get1(){
            return item1;
        }

        public P2 get2(){
            return item2;
        }
        
        public boolean equals(Object o){
            if (o instanceof Pair){
                Pair other = (Pair)o;
                return other.get1().equals(item1) && other.get2().equals(item2);
            }
            return false;
        }

    }
    
}