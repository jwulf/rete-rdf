/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package rdf;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author David Monks
 * @version 1
 */
public class DualTimeQuad extends UniTimeQuad implements DualTime {

    public static final String OPEN_CHAR = "<";
    public static final String CLOSE_CHAR = ">";
    
    private String context;
    private long validSince,emissionTime;
    
    /**
     * Converts a string to a list of DualTimeQuads.
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
                if (input.charAt(offset) == DualTimeQuad.OPEN_CHAR.charAt(0)){
                    nestCount++;
                }else if (input.charAt(offset) == '"'){
                    do {
                        offset = input.indexOf("\"", offset + 1);
                    } while (input.charAt(offset - 1) == '\\');
                }else if (input.charAt(offset) == DualTimeQuad.CLOSE_CHAR.charAt(0)){
                    nestCount--;
                }
                offset++;
            } while (nestCount > 0);
            //convert the String triple to a Triple object and add it to the list of triples.
            try{
                list.add(annotatedQuadFromString(input.substring(base, offset)));
            }catch(RDFTypeException e){
                exceptionMessage += e.getMessage()+"\n";
            }catch(RDFPartException e){
                exceptionMessage += e.getMessage()+"\n";
            }
            //progress to the next part of the string
            base = offset;
        }
        if (!exceptionMessage.equals("")){
            exceptionMessage = "*** Dual-time annotated quad errors ***\n"+exceptionMessage;
            //assume that the triple format of the string was not that of a DualTimeQuad, so try parsing them as UniTimeQuad.
            try{
                throw new RDFTypeException(exceptionMessage,UniTimeQuad.listFromString(input));
            }catch(RDFTypeException ex){
                throw new RDFTypeException(exceptionMessage+ex.getMessage(),list.size() > ex.getResults().size()
                                                                                ? list
                                                                                : ex.getResults());
            }
        }
        return list;
    }
    
    /**
     * Extracts a single DualTimeQuad from a string.
     * @param String input - a string representation of a triple
     * @return Triple triple - The object representing the triple in the input string
     * @throws RDFPartException
     * @throws RDFTypeException 
     */
    public static DualTimeQuad annotatedQuadFromString(String in) throws RDFTypeException, RDFPartException{
        Triple t;
        String[] annotations = new String[3];
        if (in.contains(DualTimeQuad.OPEN_CHAR)){
            in = in.substring(in.indexOf(DualTimeQuad.OPEN_CHAR)+1).trim();
        }
//      [...,...,...,<...>],...,...}
        t = tripleFromString(in);
        
        int offset = 1;
        offset += matchAndConsumeDataItem(in,offset).length() + 1;
//      ...,...,<...>],...,...}
        offset += matchAndConsumeDataItem(in,offset).length() + 1;
//      ...,<...>],...,...}
        offset += matchAndConsumeDataItem(in,offset).length() + 1;
//      <...>],...,...}
        
        annotations[0] = matchAndConsumeDataItem(in,offset);
        offset += annotations[0].length() + 1;
//      ,...,...}
        offset = in.indexOf(Triple.SEPARATOR, offset) + 1;
//      ...,...}
        annotations[1] = matchAndConsumeDataItem(in,offset);
        offset += annotations[1].length() + 1;
//      ...}
        try{
            annotations[2] = matchAndConsumeDataItem(in.substring(offset, in.lastIndexOf(DualTimeQuad.CLOSE_CHAR)),0);
            offset += annotations[2].length() + 1;
        } catch (StringIndexOutOfBoundsException ex){
            System.out.println(in + " : "+ ex.getMessage());
        }
//      }
        
        return new DualTimeQuad(t,
                                annotations[0].trim(),
                                annotations[1].trim(),
                                annotations[2].trim());
    }
    
    private void init(String valid) throws RDFTypeException{
        Object temp = convertToJava(valid);
        if (temp instanceof Long){
            validSince = ((Long)temp).longValue();
        }else throw new RDFTypeException("The \"valid since\" time of a triple must be a datetime, date or time: "+valid);
    }
    
    public DualTimeQuad(String sub,
                        String pred,
                        String ob,
                        String cont,
                        String valid,
                        String emission) throws RDFTypeException{
        super(sub,pred,ob,cont,emission);
        init(valid);
    }
    
    public DualTimeQuad(Triple orig,
                        String cont,
                        String valid,
                        String emission) throws RDFTypeException{
        super(orig.getSubject(),orig.getPredicate(),orig.getObject(),cont,emission);
        init(valid);
    }
    
    public DualTimeQuad(Pair<Object,RDFType> sub,
                        Pair<Object,RDFType> pred,
                        Pair<Object,RDFType> ob,
                        String cont,
                        long valid,
                        long emission){
        super(sub,pred,ob,cont,emission);
        
        validSince = valid;
    }
    
    public DualTimeQuad(Triple orig,
                        String cont,
                        long valid,
                        long emission){
        super(orig,cont,emission);
        
        validSince = valid;
    }

//    public Pair<Object, RDFType> getContext() {
//        return new Pair<Object,RDFType>(context,RDFType.URI);
//    }
//
//    public long getTime() {
//        return emissionTime;
//    }

    public long getValidTime() {
        return validSince;
    }
    
    @Override
    public Object getPart(TuplePart p) throws RDFPartException{
        try {
            return super.getPart(p);
        } catch (RDFPartException ex) {
            switch (p){
//                case CONTEXT:
//                    return context;
//                case EMISSION_TIME:
//                    return new Long(emissionTime);
                case VALID_SINCE:
                    return new Long(validSince);
                default:
                    throw new RDFPartException("Tuple does not contain this data: "+p.toString());
            }
        }
    }
    
    @Override
    public RDFType getPartType(TuplePart p) throws RDFPartException{
        try {
            return super.getPartType(p);
        } catch (RDFPartException ex) {
            switch (p){
//                case CONTEXT:
//                    return RDFType.URI;
                case VALID_SINCE:
//                case EMISSION_TIME:
                    return RDFType.DATETIME;
                default:
                    throw new RDFPartException("Tuple does not contain this data: "+p.toString());
            }
        }
    }
    
//    @Override
//    public String toRDF() throws RDFPartException, RDFTypeException{
//        return DualTimeQuad.OPEN_CHAR
//               +Triple.OPEN_CHAR+partToRDF(TuplePart.SUBJECT)
//               +Triple.SEPARATOR+partToRDF(TuplePart.PREDICATE)
//               +Triple.SEPARATOR+partToRDF(TuplePart.OBJECT)
//               +Triple.SEPARATOR+partToRDF(TuplePart.CONTEXT)+Triple.CLOSE_CHAR
//               +Triple.SEPARATOR+timeToRDF()+DualTimeQuad.CLOSE_CHAR;
//    }
    
    public String timeToRDF() throws RDFPartException, RDFTypeException {
        return partToRDF(TuplePart.VALID_SINCE)+Triple.SEPARATOR+partToRDF(TuplePart.EMISSION_TIME);
    }
//    
//    @Override
//    public String toXMLRDF() throws RDFPartException, RDFTypeException{
//        //TODO
//        throw new UnsupportedOperationException("TODO Triple-to-RDF conversion.");
//    }
//    
//    public String timeToXMLRDF() throws RDFPartException, RDFTypeException{
//        //TODO
//        throw new UnsupportedOperationException("TODO Triple-to-RDF conversion.");
//    }
    
    @Override
    public boolean combine(Triple other){
        if (equals(other)){
            if (other instanceof UniTime){
                if (other instanceof DualTime){
                    this.validSince = Math.min(this.validSince, ((DualTime)other).getValidTime());
                }
                this.emissionTime = Math.max(this.emissionTime, ((UniTime)other).getTime());
            }
            return true;
        }else{
            return false;
        }
    }
    
//    @Override
//    public boolean equals(Object o){
//        boolean equals = true;
//        if (o instanceof Quadruple){
//            equals &= context.equals(((Quadruple)o).getContext().get1());
//        }
//        return equals && super.equals(o);
//    }

}
