/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package nodes;

import java.util.List;

import org.jivesoftware.smack.XMPPException;

import rdf.RDFPartException;
import rdf.RDFType;
import rdf.RDFTypeException;
import rdf.Triple;

/**
 *
 * @author David Monks
 * @version 1
 */
public class Count extends RETENode {

    public static final String IN_STREAM = "default";
    
    private WindowQueue window;
    private long cap;
    
    public Count(String name,
                 String host,
                 String service,
                 int port,
                 long windowLength,
                 List<String> srcs) throws XMPPException{
        super(name,host,service,port);
        cap = windowLength;
        
        addStream(IN_STREAM,srcs);
        
        //TODO add customisation
        window = new WindowQueue(true,100);
    }
    
    @Override
    protected String formatTriple(Triple t) throws RDFPartException, RDFTypeException {
        return t.toRDF();
    }

    @Override
    public void loop() {
        boolean changed = false;

        Triple t;
        try {
            if ((t = retrieveData(IN_STREAM)) != null){
                window.offer(t);
                changed = true;
            }
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        }

        if (window.prune()) changed = true;

        if (changed){
            sendData(new Triple(new Triple.Pair<Object,RDFType>(Integer.toString(hashCode()),RDFType.BLANK),
                                      new Triple.Pair<Object,RDFType>("http://spql#count",RDFType.URI),
                                      new Triple.Pair<Object,RDFType>(new Integer(window.size()),RDFType.INTEGER)));
        }
    }

}
