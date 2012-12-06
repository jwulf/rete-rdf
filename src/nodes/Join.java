package nodes;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import nodes.control.Comparator;
import org.jivesoftware.smack.XMPPException;
import rdf.DualTime;
import rdf.DualTimeQuad;
import rdf.Quadruple;
import rdf.RDFPartException;
import rdf.RDFType;
import rdf.RDFTypeException;
import rdf.Triple;
import rdf.TuplePart;
import rdf.UniTime;

/**
 * In RETE Network terms, this class is an Beta node class.
 * In Relational Algebra, it is a join node.
 * @author David Monks, Rehab Albeladi
 * @version 2
 */
public class Join extends RETENode {
    
    private static final long DEFAULT_WINDOW_SIZE = 100;
    
    public static final String LEFT_STREAM = "left";
    public static final String RIGHT_STREAM = "right";
    public static final int LEFT = 1;
    public static final int RIGHT = 2;
    
    private List<Condition> conditions;
    private Template template;
    
    private Window left;
    private Window right;
    private Thread lThread;
    private Thread rThread;
    
    /**
     * Create a join node, with a defined identifier and connection to a defined XMPP server.
     * The join node is provided with the jabber address of sources of data, each including which side the source is supposed to feed (left or right).
     * It is also provided with the conditions the join must satisfy, and the template that the output of the node must conform to.
     * Finally, it is provided with the extent and limiting factors of the size of the left and right sliding windows.
     * @param String name
     * @param String host
     * @param String service
     * @param int port
     * @param List<String> sources
     * @param List<Condition> cndtns
     * @param Template t
     * @param boolean leftSizeBased - true if the left window is to be limited based on size, false if it is to be based on time
     * @param boolean rightSizeBased - true if the right window is to be limited based on size, false if it is to be based on time
     * @param long leftWindowSize - the value of the constraint on left window size/age
     * @param long rightWindowSize - the value of the constraint on right window size/age
     * @throws XMPPException 
     */
    public Join(String name,
                String host,
                String service,
                int port,
                List<String> sources,
                List<Condition> cndtns,
                Template t,
                boolean leftSizeBased,
                boolean rightSizeBased,
                long leftWindowSize,
                long rightWindowSize)throws XMPPException{
        super(name,host,service,port);
        template = t;
        
        //Separate the sources into a left input list and a right input list, based on the substring after the colon in the source address.
        List<String> lSources = new ArrayList<String>();
        List<String> rSources = new ArrayList<String>();
        for (String source : sources){
            String[] temp = source.split(":");
            if (temp[1].equalsIgnoreCase(LEFT_STREAM))
                lSources.add(source);
            if (temp[1].equalsIgnoreCase(RIGHT_STREAM))
                rSources.add(source);
        }
        //check that both sides of the join node have sources.
        //if only one side has sources, then the node joins data within one stream.
        //otherwise it sets up two separate windows and two separate input feeds, with size/age limitations as specified in the parameters.
        if (lSources.isEmpty()){
            addStream(RIGHT_STREAM,rSources);
            WindowQueue queue = new WindowQueue(rightSizeBased,rightWindowSize);
            left = new Window(RIGHT_STREAM,LEFT,queue);
            right = new Window(RIGHT_STREAM,LEFT,queue);
        }else if (rSources.isEmpty()){
            addStream(LEFT_STREAM,lSources);
            WindowQueue queue = new WindowQueue(leftSizeBased,leftWindowSize);
            left = new Window(LEFT_STREAM,LEFT,queue);
            right = new Window(LEFT_STREAM,LEFT,queue);
        }else{
            addStream(LEFT_STREAM,lSources);
            addStream(RIGHT_STREAM,rSources);
            left = new Window(LEFT_STREAM,LEFT,leftSizeBased,leftWindowSize);
            right = new Window(RIGHT_STREAM,RIGHT,rightSizeBased,rightWindowSize);
        }
        //make the left and right windows aware of each other.
        left.setPartner(right);
        right.setPartner(left);
        //set the conditions of the join.
        conditions = cndtns;
    }
    
    /**
     * Create a join node, with a defined identifier and connection to a defined XMPP server.
     * The join node is provided with the jabber address of sources of data, each including which side the source is supposed to feed (left or right).
     * It is also provided with the conditions the join must satisfy, and the template that the output of the node must conform to.
     * Finally, it is provided with the extent and limiting factors of the size of the sliding windows.
     * @param String name
     * @param String host
     * @param String service
     * @param int port
     * @param List<String> sources
     * @param List<Condition> cndtns
     * @param Template t
     * @param boolean sizeBased - true if the window is to be limited based on size, false if it is to be based on time
     * @param long windowSize - the value of the constraint on the window size/age
     * @throws XMPPException 
     */
    public Join(String name,
                String host,
                String service,
                int port,
                List<String> sources,
                List<Condition> cndtns,
                Template t,
                boolean sizeBased,
                long windowSize) throws XMPPException{
        this(name,
             host,
             service,
             port,
             sources,
             cndtns,
             t,
             sizeBased,sizeBased,
             windowSize,windowSize);
    }
    
    /**
     * Create a join node, with a defined identifier and connection to a defined XMPP server.
     * The join node is provided with the jabber address of sources of data, each including which side the source is supposed to feed (left or right).
     * It is also provided with the conditions the join must satisfy, and the template that the output of the node must conform to.
     * @param String name
     * @param String host
     * @param String service
     * @param int port
     * @param List<String> sources
     * @param List<Condition> cndtns
     * @param Template t
     * @throws XMPPException 
     */
    public Join(String name,
                String host,
                String service,
                int port,
                List<String> sources,
                List<Condition> cndtns,
                Template t) throws XMPPException{
        this(name,
             host,
             service,
             port,
             sources,
             cndtns,
             t,
             true,true,DEFAULT_WINDOW_SIZE,DEFAULT_WINDOW_SIZE);
    }

    /**
     * Dictates what format of Triple the node outputs, in this case it is to N-triples notation.
     * @param Triple t - The triple to be formatted.
     * @return String formattedTriple - The stringified form of the Triple.
     * @throws RDFPartException
     * @throws RDFTypeException 
     */
    @Override
    protected String formatTriple(Triple t) throws RDFPartException, RDFTypeException {
        return t.toRDF();
    }
    
    /**
     * Overridden start function, adding the functionality to start the two window threads, allowing the node to function.
     */
    @Override
    public void start(){
        super.start();
        lThread = new Thread(left);
        rThread = new Thread(right);
        lThread.start();
        rThread.start();
    }
    
    /**
     * Overridden stop function, adding the functionality to stop the two window threads.
     */
    @Override
    public void stop(){
        super.stop();
        boolean waiting = true;
        while (waiting)
            try {
                lThread.join();
                rThread.join();
                waiting = false;
            } catch (InterruptedException ex) {}
    }

    /**
     * The main looping method of the join is useless, as the behaviour is carried out by the Window objects.
     */
    @Override
    public void loop() {
        try {
            Thread.sleep(60000);
        } catch (InterruptedException ex) {}
    }
    
    //Nested Classes
    /**
     * A class for maintaining a sliding window of a join node, including all joining functionality.
     */
    private class Window extends Thread{
        
        private String stream;
        private int side;
        private WindowQueue window;
        private Window other;
        
        /**
         * Creates one of the sliding windows as part of a join node.
         * @param String strm - the name of stream the window fetches data from
         * @param int p - the "side" of the window (1 = left, 2 = right)
         * @param boolean sizeBased - true if the window is to be size limited, false if it is to be age limited.
         * @param long windowSize  - the size or age limit of the window.
         */
        public Window(String strm,int p,boolean sizeBased,long windowSize){
            stream = strm;
            side = p;
            window = new WindowQueue(sizeBased,windowSize);
            other = null;
        }
        
        /**
         * Creates one of the sliding windows as part of a join node.
         * @param String strm - the name of stream the window fetches data from
         * @param int p - the "side" of the window (1 = left, 2 = right)
         * @param WindowQueue w - A previously constructed window queue.
         */
        public Window(String strm,int p,WindowQueue w){
            stream = strm;
            side = p;
            window = w;
            other = null;
        }
        
        /**
         * Links this window to a partner window.
         * @param Window o - the "other side" of the join node, with which the window joins data
         */
        public void setPartner(Window o){
            other = o;
        }

        /**
         * The Main operation of this window.
         * After retrieving a Triple, check if a previous triple matches it exactly. If it does, replace the old triple with the new one.
         * If it does not, send it to the partner window to check for any joins.
         * After the check, add the triple to this side's window queue.
         */
        public void run() {
            while (isRunning()){
                try {
                    Triple t;
                    //retrieve next available triple
                    while ((t = retrieveData(stream)) == null) ;
                    synchronized(Join.this){
                        //check if a previous triple matches, and replace old with new if necessary.
                        //if no replace happens then...
                        if (!window.combine(t)){
                            try {
                                //perform join with partner window.
                                other.join(t);
                                //if there is just one window queue then the "partner" window will reference the same window.
                                if (other.window == window)
                                    //joins must be checked both ways round to makes sure all joins are caught.
                                    join(t);
                                //add the new triple to the window.
                                window.offer(t);
                             /*
                              * Drops triples that exceed the window capacity, as defined at the creation of the window.
                              * Done here to maintain low window size after a change to its contents (commented out to reduce load on CPU, assuming system memory can cope with a few surplus triples).
                              */
//                                window.prune();
                            } catch (RDFPartException ex) {
                                System.out.println(ex.getMessage());
                            }
                        }
                    }
                } catch (InterruptedException ex) {
                    System.out.println(ex.getMessage());
                }
            }
        }
        
        /**
         * Test the received Triple against the contents this window's queue of data.
         * If the Triple triggers any joins, then a new triple is created from the node's template.
         * @param Tiple o - triple to be compared to the contents of this window
         * @return boolean joined - true if a join occurred
         * @throws RDFPartException 
         */
        protected boolean join(Triple o) throws RDFPartException{
         /* 
          * Drops triples that exceed the window capacity, as defined at the creation of the window.
          * Done here so that the window is accurate at the time of the join.
          */
            window.prune();
            boolean joined = false;
            //for each Triple in the window queue...
            for (Object ob : window){
                Triple t = (Triple) ob;
                //set default value of whether the received triple and current triple from the window meet the conditions to true.
                boolean join = true;
                //for each of the conditions of the join...
                for (Condition cond : conditions)
                    //if the received triple and the current triple from the window do not meet the current join condition, change the boolean flag to false and break from the loop over the conditions.
                    //otherwise, continue until all conditions have been met.
                    if (!cond.check(t.getPart(cond.get(side)), side, o.getPart(cond.get((side == LEFT) ? RIGHT : LEFT)))){
                    	join = false;
                        break;
                    }
                //if all conditions were met...
                if (join){
                    //try to apply the template to the pair of triples, sending the resultant triple out on the output stream (handled by the underlying RETENode).
                    //also set the value to be returned to true, as a join has occured.
                    try {
                        sendData(template.apply(t,o,side));
                        joined = true;
                    } catch (RDFTypeException ex) {
                        System.out.println("Template requests unavailable part of tuple: "+ex.getMessage());
                    } catch (InputException ex) {
                        System.out.println(ex.getMessage());
                    }
                }
            }
            //return the flag indicating whether a join has occured or not.
            return joined;
        }
        
    }
    
    /**
     * A class for storing and modeling join node conditions.
     * Has the ability to take two triples and determine if they meet the stored condition.
     */
    public static class Condition{
        
        private TuplePart first,second;
        private Comparator comparator;
        
        /**
         * Defines a condition, as a comparison between part of the first tuple and part of the second.
         * @param TuplePart f - part of first triple to be compared
         * @param c - the object that dictates the comparison between the first and second parts
         * @param s - part of second triple to be compared
         */
        public Condition(TuplePart f, Comparator c, TuplePart s){
            first = f;
            comparator = c;
            second = s;
        }
        
        /**
         * Returns the part to be compared of the triple indicated by the parameter.
         * @param int p - the "side" of the join to fetch from, left to right (1 = first, 2 = second).
         * @return TuplePart part - the part of the indicated triple that is to be checked against the part in the other.
         * @throws RDFPartException 
         */
        public TuplePart get(int p) throws RDFPartException{
            switch (p){
                case LEFT:
                    return first;
                case RIGHT:
                    return second;
                default:
                    throw new RDFPartException("Condition part must be Condition.FIRST or Condition.SECOND.");
            }
        }
        
        /**
         * Applies the comparator to the relevant parts of the 
         * @param f
         * @param p
         * @param s
         * @return
         * @throws RDFPartException 
         */
        public boolean check(Object f,int p,Object s) throws RDFPartException{
            switch (p){
                case LEFT:
                    return comparator.compare(f,s);
                case RIGHT:
                    return comparator.compare(s,f);
                default:
                    throw new RDFPartException("Condition part must be Condition.FIRST or Condition.SECOND.");
            }
        }
        
    }
    
    public static class Template{
        
        private TuplePart newSub,newPred,newOb;
        private int subSrc,predSrc,obSrc;
        
        public Template(TuplePart p1,int t1,TuplePart p2,int t2,TuplePart p3,int t3) throws RDFPartException{
            if (((p1 == null && (t1 != LEFT || t1 != RIGHT)) || (p1 != null && (t1 == LEFT || t1 == RIGHT)))
             && ((p2 == null && (t2 != LEFT || t2 != RIGHT)) || (p2 != null && (t2 == LEFT || t2 == RIGHT)))
             && ((p3 == null && (t3 != LEFT || t3 != RIGHT)) || (p3 != null && (t3 == LEFT || t3 == RIGHT)))){
                newSub = p1;
                subSrc = t1;
                newPred = p2;
                predSrc = t2;
                newOb = p3;
                obSrc = t3;
            }else
                throw new RDFPartException("For each template part P:T, both P and T must be 'null' or neither can be.  SHOULD NOT REACH HERE!");
        }
        
        private Triple.Pair<Object,RDFType> extractPart(Triple t,TuplePart p) throws RDFTypeException{
            switch (p){
                case SUBJECT:
                    return t.getSubject();
                case PREDICATE:
                    return t.getPredicate();
                case OBJECT:
                    return t.getObject();
                case CONTEXT:
                    if (t instanceof Quadruple)
                        return ((Quadruple)t).getContext();
                    else
                        throw new RDFTypeException("Tuple type does not support Context or Graph data.");
                case VALID_SINCE:
                    if (t instanceof DualTime)
                        return new Triple.Pair<Object,RDFType>(new Long(((DualTime)t).getValidTime()),RDFType.DATETIME);
                    else
                        throw new RDFTypeException("Tuple type does not support \"Valid Since\" Time Annotations.");
                case EMISSION_TIME:
                    if (t instanceof UniTime)
                        return new Triple.Pair<Object,RDFType>(new Long(((UniTime)t).getTime()),RDFType.DATETIME);
                    else
                        throw new RDFTypeException("Tuple type does not support Time Annotations.");
                default:
                    System.out.println("Cannot have a blank part linked to a specific input stream.  SHOULD NOT GET HERE!");
                    System.exit(1);
            }
            return null;
        }
        
        public Triple apply(Triple t,Triple o,int part)
                throws RDFTypeException, InputException, RDFPartException{
            Triple newT;
            Triple.Pair<Object,RDFType> sub,pred,ob;
            switch (part){
                case LEFT:
                    switch (subSrc){
                        case LEFT:
                            sub = extractPart(t,newSub);break;
                        case RIGHT:
                            sub = extractPart(o,newSub);break;
                        default:
                            sub = new Triple.Pair<Object,RDFType>(Integer.toHexString(hashCode()),RDFType.BLANK);
                    }
                    switch (predSrc){
                        case LEFT:
                            pred = extractPart(t,newPred);break;
                        case RIGHT:
                            pred = extractPart(o,newPred);break;
                        default:
                            pred = new Triple.Pair<Object,RDFType>(Integer.toHexString(hashCode()),RDFType.BLANK);
                    }
                    switch (obSrc){
                        case LEFT:
                            ob = extractPart(t,newOb);break;
                        case RIGHT:
                            ob = extractPart(o,newOb);break;
                        default:
                            ob = new Triple.Pair<Object,RDFType>(Integer.toHexString(hashCode()),RDFType.BLANK);
                    }
                    break;
                case RIGHT:
                    switch (subSrc){
                        case LEFT:
                            sub = extractPart(o,newSub);break;
                        case RIGHT:
                            sub = extractPart(t,newSub);break;
                        default:
                            sub = new Triple.Pair<Object,RDFType>(Integer.toHexString(hashCode()),RDFType.BLANK);
                    }
                    switch (predSrc){
                        case LEFT:
                            pred = extractPart(o,newPred);break;
                        case RIGHT:
                            pred = extractPart(t,newPred);break;
                        default:
                            pred = new Triple.Pair<Object,RDFType>(Integer.toHexString(hashCode()),RDFType.BLANK);
                    }
                    switch (obSrc){
                        case LEFT:
                            ob = extractPart(o,newOb);break;
                        case RIGHT:
                            ob = extractPart(t,newOb);break;
                        default:
                            ob = new Triple.Pair<Object,RDFType>(Integer.toHexString(hashCode()),RDFType.BLANK);
                    }
                    break;
                default:
                    throw new InputException("SHOULD NOT REACH HERE!!");
            }
            if (t instanceof DualTimeQuad && o instanceof DualTimeQuad)
                return new DualTimeQuad(sub,pred,ob,
                                        (part == subSrc) ? (String)t.getPart(TuplePart.CONTEXT) : (String)o.getPart(TuplePart.CONTEXT),
                                        Math.max(((Long)t.getPart(TuplePart.VALID_SINCE)).longValue(),((Long)o.getPart(TuplePart.VALID_SINCE)).longValue()),
                                        Math.max(((Long)t.getPart(TuplePart.EMISSION_TIME)).longValue(),((Long)o.getPart(TuplePart.EMISSION_TIME)).longValue()));
            else
                return new Triple(sub,pred,ob);
        }
        
    }
    
    public static class InputException extends Exception {
        
        public InputException(String message) {
            super("Join iputs are either Join.LEFT or Join.RIGHT (integer 1 or 2 respectively). "+message);
        }
        
        public InputException(){
            this("");
        }
        
    }
}


