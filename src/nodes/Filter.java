package nodes;

/**
 * In RETE Network terms, this class is an Alpha node class.
 * In Relational Algebra, it is a select node.
 * In simple terms, it is a filter node.
 * @author David Monks, Rehab Albeladi
 * @version 2
 */
import java.util.List;

import nodes.control.Comparator;
import nodes.control.Equality;

import org.jivesoftware.smack.XMPPException;

import rdf.RDFPartException;
import rdf.RDFType;
import rdf.RDFTypeException;
import rdf.Triple;
import rdf.TuplePart;

public class Filter extends RETENode {
    
    public static final String IN_STREAM = "default";
    
    private TuplePart part;
    private TuplePart part2;
    private Object value;
    private RDFType type;
    private Comparator comparator;
    
    /**
     * Create a filter node, with a defined identifier and connection to a defined XMPP server.
     * The filter node is provided with the jabber address of sources of data, which part of the data is to be filtered on, and what kind of filtering is to be done.
     * The item the data is to be compared against is set by one of three public constructors.
     * @param String name
     * @param String host
     * @param String service
     * @param int port
     * @param List<String> sources
     * @param TuplePart p
     * @param Comparator c
     * @throws XMPPException 
     */
    private Filter(String name,
                   String host,
                   String service,
                   int port,
                   List<String> sources,
                   TuplePart p,
                   Comparator c) throws XMPPException{               
        super(name,host,service,port);
        part = p;
        comparator = c;
        addStream(IN_STREAM,sources);
    }
    /**
     * Constructs a Filter that compares one part of each tuple processed with a second part of the same tuple.
     * @param TuplePart p
     * @param  v
     * @param Comparator c 
     */
    public Filter(String name,
                  String host,
                  String service,
                  int port,
                  List<String> sources,
                  TuplePart p,
                  String v,
                  Comparator c) throws XMPPException, RDFTypeException{
        this(name,host,service,port,sources,p,c);
        value = Triple.convertToJava(v);
        part2 = null;
        type = null;
    }
    /**
     * Constructs a Filter that compares one part of each tuple processed with a second part of the same tuple.
     * @param TuplePart p
     * @param TuplePart v
     * @param Comparator c 
     */
    public Filter(String name,
                  String host,
                  String service,
                  int port,
                  List<String> sources,
                  TuplePart p,
                  TuplePart v,
                  Comparator c) throws XMPPException{
        this(name,host,service,port,sources,p,c);
        part2 = v;
        value = null;
        type = null;
    }
    /**
     * Constructs a Filter that compares one part of each tuple processed with a second part of the same tuple.
     * @param TuplePart p
     * @param TuplePart v
     * @param Comparator c 
     */
    public Filter(String name,
                  String host,
                  String service,
                  int port,
                  List<String> sources,
                  TuplePart p,
                  RDFType v,
                  Equality c) throws XMPPException{
        this(name,host,service,port,sources,p,c);
        type = v;
        part2 = null;
        value = null;
    }

    /**
     * As long as the node is active and running, this block will be repeated.
     * Retrieve the next piece of RDF from the input queue.
     * Check which kind of filter is to be performed (to literal, intra tuple or type check).
     * Apply filter, sending data to listening nodes if they pass the filter requirements.
     */
    @Override
    public void loop() {
        try {
            Triple t;
            if ((t = retrieveData(IN_STREAM)) != null) try{
                if (value != null){

                    if (comparator.compare(t.getPart(part), value)){
                        sendData(t);
                    }
                }else if (part2 != null){
                    if (comparator.compare(t.getPart(part), t.getPart(part2))){
                        sendData(t);
                    }
                }else
                    if (comparator.compare(t.getPartType(part), type)){
                        sendData(t);
                    }
            } catch (RDFPartException ex) {
                System.out.println("Triple does not contain part "+part.toString()+".  Contact system administrator: something terrible has happened!");
            }
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        }
    }

    /**
     * Formats triple data into an RDF string (rather than XMLRDF), for inter-node communication.
     * @param Triple t
     * @return String rdf
     */
    @Override
    protected String formatTriple(Triple t) throws RDFPartException, RDFTypeException{
        return t.toRDF();
    }
    
}
