/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package rdf;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author davidlmonks
 */
public class UniTimeQuad extends Triple implements UniTime,Quadruple {

    public static final String OPEN_CHAR = "<";
    public static final String CLOSE_CHAR = ">";
    
    private String context;
    private long emissionTime;
    
    public static List<Triple> listFromString(String input) throws RDFTypeException{
        int base = 0;
        int offset = 0;
        List<Triple> list = new ArrayList<Triple>();
        
        input = input.trim();
        String exceptionMessage = "";
        while (offset < input.length()){
            int nestCount = 0;
            do {
                if (input.charAt(offset) == UniTimeQuad.OPEN_CHAR.charAt(0)){
                    nestCount++;
                }else if (input.charAt(offset) == '"'){
                    do {
                        offset = input.indexOf("\"", offset + 1);
                    } while (input.charAt(offset - 1) == '\\');
                }else if (input.charAt(offset) == UniTimeQuad.CLOSE_CHAR.charAt(0)){
                    nestCount--;
                }
                offset++;
            } while (nestCount > 0);
            try{
                list.add(annotatedQuadFromString(input.substring(base, offset)));
            }catch(RDFTypeException e){
                exceptionMessage += e.getMessage()+"\n";
            }catch(RDFPartException e){
                exceptionMessage += e.getMessage()+"\n";
            }
            base = offset;
        }
        if (!exceptionMessage.equals("")){
            exceptionMessage = "*** Uni-time annotated quad errors ***\n"+exceptionMessage;
            try{
                throw new RDFTypeException(exceptionMessage,Triple.listFromString(input));
            }catch(RDFTypeException ex){
                throw new RDFTypeException(exceptionMessage+ex.getMessage(),list.size() > ex.getResults().size()
                                                                                ? list
                                                                                : ex.getResults());
            }
        }
        return list;
    }
    
    public static UniTimeQuad annotatedQuadFromString(String in) throws RDFTypeException, RDFPartException{
        Triple t;
        String[] annotations = new String[2];
        if (in.contains(UniTimeQuad.OPEN_CHAR)){
            in = in.substring(in.indexOf(UniTimeQuad.OPEN_CHAR)+1).trim();
        }
//      [...,...,...,<...>],...}
        t = tripleFromString(in);
        
        int offset = 1;
        offset += matchAndConsumeDataItem(in,offset).length() + 1;
//      ...,...,<...>],...}
        offset += matchAndConsumeDataItem(in,offset).length() + 1;
//      ...,<...>],...}
        offset += matchAndConsumeDataItem(in,offset).length() + 1;
//      <...>],...}
        
        annotations[0] = matchAndConsumeDataItem(in,offset);
        offset += annotations[0].length() + 1;
//      ,...}
        offset = in.indexOf(Triple.SEPARATOR, offset) + 1;
//      ...}
        annotations[1] = matchAndConsumeDataItem(in,offset);
        offset += annotations[1].length() + 1;
//      }
        
        return new UniTimeQuad(t,
                                annotations[0].trim(),
                                annotations[1].trim());
    }
    
    private void init(String cont,
                      String emission) throws RDFTypeException{
        Object temp;
        temp = convertToJava(cont);
        if (temp instanceof String){
            context = (String) temp;
        }else throw new RDFTypeException("The context of a triple must be a URI: "+cont);
        temp = convertToJava(emission);
        if (temp instanceof Long){
            emissionTime = ((Long)temp).longValue();
        }else throw new RDFTypeException("The emission time of a triple must be a datetime, date or time: "+emission);
    }
    
    public UniTimeQuad(String sub,
                        String pred,
                        String ob,
                        String cont,
                        String emission) throws RDFTypeException{
        super(sub,pred,ob);
        init(cont,emission);
    }
    
    public UniTimeQuad(Pair<Object,RDFType> sub,
                        Pair<Object,RDFType> pred,
                        Pair<Object,RDFType> ob,
                        String cont,
                        String emission) throws RDFTypeException{
        super(sub,pred,ob);
        init(cont,emission);
    }
    
    public UniTimeQuad(Triple orig,
                        String cont,
                        String emission) throws RDFTypeException{
        super(orig.getSubject(),orig.getPredicate(),orig.getObject());
        init(cont,emission);
    }
    
    public UniTimeQuad(Pair<Object,RDFType> sub,
                        Pair<Object,RDFType> pred,
                        Pair<Object,RDFType> ob,
                        String cont,
                        long emission){
        super(sub,pred,ob);
        
        context = cont;
        emissionTime = emission;
    }
    
    public UniTimeQuad(Triple orig,
                        String cont,
                        long emission){
        super(orig.getSubject(),orig.getPredicate(),orig.getObject());
        
        context = cont;
        emissionTime = emission;
    }

    public Pair<Object, RDFType> getContext() {
        return new Pair<Object,RDFType>(context,RDFType.URI);
    }

    public long getTime() {
        return emissionTime;
    }
    
    @Override
    public Object getPart(TuplePart p) throws RDFPartException{
        try {
            return super.getPart(p);
        } catch (RDFPartException ex) {
            switch (p){
                case CONTEXT:
                    return context;
                case EMISSION_TIME:
                    return new Long(emissionTime);
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
                case CONTEXT:
                    return RDFType.URI;
                case EMISSION_TIME:
                    return RDFType.DATETIME;
                default:
                    throw new RDFPartException("Tuple does not contain this data: "+p.toString());
            }
        }
    }
    
    @Override
    public String toRDF() throws RDFPartException, RDFTypeException{
        return UniTimeQuad.OPEN_CHAR
               +Triple.OPEN_CHAR+partToRDF(TuplePart.SUBJECT)
               +Triple.SEPARATOR+partToRDF(TuplePart.PREDICATE)
               +Triple.SEPARATOR+partToRDF(TuplePart.OBJECT)
               +Triple.SEPARATOR+partToRDF(TuplePart.CONTEXT)+Triple.CLOSE_CHAR
               +Triple.SEPARATOR+timeToRDF()+UniTimeQuad.CLOSE_CHAR;
    }
    
    public String timeToRDF() throws RDFPartException, RDFTypeException {
        return partToRDF(TuplePart.VALID_SINCE);
    }
    
    @Override
    public String toXMLRDF() throws RDFPartException, RDFTypeException{
        //TODO
        throw new UnsupportedOperationException("TODO Triple-to-RDF conversion.");
    }
    
    public String timeToXMLRDF() throws RDFPartException, RDFTypeException{
        //TODO
        throw new UnsupportedOperationException("TODO Triple-to-RDF conversion.");
    }
    
    @Override
    public boolean combine(Triple other){
        if (equals(other)){
            if (other instanceof UniTime){
                this.emissionTime = Math.max(this.emissionTime, ((UniTime)other).getTime());
            }
            return true;
        }else{
            return false;
        }
    }
    
    @Override
    public boolean equals(Object o){
        boolean equals = true;
        if (o instanceof Quadruple){
            equals &= context.equals(((Quadruple)o).getContext().get1());
        }
        return equals && super.equals(o);
    }

}