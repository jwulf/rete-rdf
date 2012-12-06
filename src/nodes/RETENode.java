/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package nodes;

/**
 *
 * @author David Monks, Rehab Albeladi
 * @version 2
 */

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;

import org.jivesoftware.smack.*;
import org.jivesoftware.smack.filter.PacketFilter;
import org.jivesoftware.smack.packet.Message;
import org.jivesoftware.smack.packet.Packet;
import org.jivesoftware.smack.packet.Presence;

import rdf.DualTimeQuad;
import rdf.RDFPartException;
import rdf.RDFTypeException;

import rdf.DualTime;
import rdf.Triple;
import rdf.TuplePart;
import rdf.UniTime;

abstract public class RETENode implements Runnable {

    private static int PACKET_SIZE = 1000;
    private static int PACKET_AGE = 1000;
    
    private static final String SOURCES = "sources";
    
    //Producer/Consumer
    private Map<String,Queue<Triple>> input;
    private boolean running;
    private Thread mainThread;
    private PacketSender sendThread;
    
    //XMPP
    private String host;
    private int port;
    private String service;
    private ConnectionConfiguration config;
    private XMPPConnection conn;
    private Roster roster;
    private TimeoutListener timeout;
    private String name;
    private String pass;
    
    //Publish/Subscribe
    private Map<String,Chat> listeners;
    private Map<String,InputListener> feeds;
    
    /**
     * Creates a new RETE node.
     * This involves initialising the input and output producer/consumer buffers, creating and connecting to the node's jabber account using the listeg arguments, and initialising the Publish/Subscribe framework for the node (the XMPP roster and the URI to smack object maps for output listeners and feeds).
     * @param String n - the jabber username of the node
     * @param String h - the URL of the jabber host
     * @param String s - the name of the jabber service on the host
     * @param int p - the jabber port on the host
     */
    public RETENode(String n,
                    String h,
                    String s,
                    int po) throws XMPPException{
    //Producer/Consumer
        input = new HashMap<String,Queue<Triple>>();
        running = false;

    //XMPP - DEBUG
        System.setProperty("smack.debugEnabled", "true");
        XMPPConnection.DEBUG_ENABLED = true;
        
    //XMPP
        //store relevant account and connection information.  The password is randomized at account creation time for account security.
        name = n;
        pass = Long.toHexString((new Random()).nextLong());
        host = h;
        service = s;
        port = po;
        //create a connection to the XMPP server
        config = new ConnectionConfiguration(host, port,service);
        conn = new XMPPConnection(config);
        //add listeners to the connection: SubscribeListener listens for subscription presence packets and responds, FeedsListener listens for the first message in an incoming "chat" and creates a dedicated MessageListener for that chat.
        conn.addPacketListener(new SubscribeListener(),new SubscribeFilter());
        conn.addPacketListener(new FeedsListener(), new FeedsFilter());
        //connect to the XMPP server, then create a new account for the node.
        conn.connect();
        try{
            conn.getAccountManager().createAccount(name, pass);
        } catch (XMPPException ex){
            System.out.println(ex.getMessage()+" on name "+name+", please manually deregister previous account before continuing.");
            System.exit(1);
        }
        //add a hook so that, when the JVM housing the node closes, the node's jabber account is deleted.  This is necessary because of the process above that creates the account at the same time as the node is created, which requires there to be no other account with the same name on the server.
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run(){
                try {
                    conn.getAccountManager().deleteAccount();
                    conn.disconnect();
                } catch (XMPPException ex) {
                    System.out.println("Unable to deregister \""+name+"\"!  Manual deletion required.");
                }
            }
        });
        //login to the XMPP server using the newly created account for the node
        conn.login(name,pass);
        
    //Publish/Subscribe initialisation
        //Initialise maps
        listeners = new HashMap<String,Chat>();
        feeds = new HashMap<String,InputListener>();
        roster = conn.getRoster();
        //indicate that we want to handle the subscription mechanism manually
        roster.setSubscriptionMode(Roster.SubscriptionMode.manual);
        //create a group in which to store sources this node is subscribed to
        roster.createGroup(SOURCES);
        //attach a TimeoutListener object (see the nested class below) to the roster to detect when source nodes go offline
        roster.addRosterListener(timeout = new TimeoutListener());
    }
   
    /**
     * Called from subclasses in order to define streams and the sources they are fed by (e.g. the single or "default" stream for filter/select/alpha nodes, and the "left" and "right" streams in join/beta nodes).  Creates an input buffer and adds all relevant sources to it from the list passed in as an argument.
     * @param String stream - the name of the input stream that the subclass can use to access its data.
     * @param List<String> srcs - the list of "addresses" of nodes that are sources of the stream being added.
     */
    protected void addStream(String stream,List<String> srcs) throws XMPPException{
        input.put(stream, new ArrayDeque<Triple>());
        for (String src : srcs){
            String[] source = src.split(":");
            if (source.length > 1 && stream.substring(0,1).equalsIgnoreCase(source[1].substring(0,1))){
                addSource(src);
            }
        }
    }
    /**
     * Adds the node "src" as a source for the appropriate stream, and subscribes this node to "src" node via the XMPP presence mechanic.
     * @param String src - the name and intended stream of the source node being added
     */
    private void addSource(String src) throws XMPPException{
            String[] source = src.split(":");
            String[] groups = {SOURCES};
            synchronized(feeds){
                feeds.put(src, null);
            }
            roster.createEntry(source[0], source[0].substring(0,source[0].indexOf("@")), groups);
    }
    
    /**
     * A class for processing the first message in a "chat" from a source node.
     */
    private class FeedsListener implements PacketListener {

        /**
         * Processes a packet to determine its source and set up a dedicated MessageListener to handle all further messages from that source.
         * @param Packet packet - the first packet received from one of the sources since it was added as a source
         */
        public void processPacket(Packet packet) {
            //cast packet to Message
            Message msg = (Message) packet;
            //set variable "source" to null (so that it has been defined, so that it can be checked whether the following code has set it or not later)
            String[] source = null;
            //check that the sender of the message is an expected source of data and that this is the first message from that sender (feeds.get(s) == null).
            synchronized(feeds){
                for (String s : feeds.keySet())
                    //fist step is to find the feed that corresponds to the "from" field of the message.
                    //Message addresses may be followed by "/smack/", and the name in the "feeds" map is followed by the name of the stream.
                    //As such, the ends of the addresses must be cropped during the check for equivalence, thereby not allowing the use of a simple equality check.
                    if (feeds.get(s) == null && msg.getFrom().startsWith(s.substring(0, s.indexOf(":")))){
                        //if this is the first message received from this source then the feed will not yet have a listener dedicated to it.
                        //Assign the adress and stream name of the source to the variable "source", indicating that the message needs further processing.
                        source = s.split(":");
                        break;
                    }
                //if source has been assigned, then a data listener must be dedicated to chat thread that the message has been received on.
                if (source != null){
                    //create a listener that passes all data to the input buffer of the feed's stream (source[1] contains the stream name)
                    InputListener il = new InputListener(input.get(source[1]));
                    //register the InputListener as dedicated to the appropriate feed 
                    feeds.put(source[0]+":"+source[1], il);
                    //dedicate the InputListener to processing messages from the chat thread of the current message (identified by msg.getThread())
                    conn.getChatManager().createChat(source[0],msg.getThread(), il);
                    //pass the message to the newly dedicated InputListener for standard message processing
                    il.processMessage(null, msg);
                }
                //If there are no feeds left without dedicated listeners, then remove this packet listener from the XMPP connection, as it is no longer needed.
                if (!feeds.containsValue(null) && !feeds.isEmpty()){
                    conn.removePacketListener(this);
                }
            }
        }
        
    }
    /**
     * A class for filtering packets based on whether or not they are the first message received from a recognised source.
     */
    private class FeedsFilter implements PacketFilter {

        /**
         * Processes a packet to determine whether it is from an expected source, and whether it is the first message received from that source.
         * @param Packet packet - any packet received by the node through the connection to the XMPP server.
         * @return boolean - true if the packet matches the filter requirements, false otherwise.
         */
        public boolean accept(Packet packet) {
            synchronized(feeds){
                for (String s : feeds.keySet())
                    if (feeds.get(s) == null && packet.getFrom().startsWith(s.substring(0, s.indexOf(":")))){
                        return packet instanceof Message;
                    }
            }
            return false;
        }
        
    }
    
    /**
     * A class for detecting when a source node goes offline, and subsequently restarting the threads that check to make sure all sources are online.
     */
    private class TimeoutListener implements RosterListener {

        private SourceConfirmer thread;
        
        /**
         * Checks to ensure that the SourceConfirmer thread is not currently running, then reinitialises it and sets it running.
         */
        public void start(){
            if (thread == null || thread.getState().equals(Thread.State.TERMINATED)){
                thread = new SourceConfirmer();
                thread.start();
            }
        }
        
        public void entriesAdded(Collection<String> clctn) {}
        public void entriesUpdated(Collection<String> clctn) {}
        public void entriesDeleted(Collection<String> clctn) {}
        /**
         * If the change is an entry become anything other than "available", reset the all references to the source the entry refers to as if it had never been subscribed to or communicated with, then start trying to subscribe to it again.
         * @param Presence prsnc - the presence packet that reported the change in a source's status.
         */
        public void presenceChanged(Presence prsnc) {
            //if the source is no longer available...
            if (!prsnc.getType().equals(Presence.Type.available)){
                try {
                    String[] groups = {SOURCES};
                    int index;
                    String user;
                    //format the source name to remove the arbitrary "/smack/" part that may follow it.
                    if ((index = prsnc.getFrom().indexOf("/")) > 0)
                        user = prsnc.getFrom().substring(0,index);
                    else
                        user = prsnc.getFrom();
                    //remove the listener dedicated to the feed from the now-unavailable source (when a new node replaces the downed node, it will have the same name but start a new chat thread that the old listener will not "hear")
                    synchronized(feeds){for (String s : feeds.keySet())
                        if (s.startsWith(prsnc.getFrom())){
                            feeds.put(s, null);
                            break;
                        }
                    }
                    //re-apply the FeedsListener from above.  If the listener already exists then this function does nothing.
                    conn.addPacketListener(new FeedsListener(), new FeedsFilter());
                    //Remove the source from the roster, if it still exists, then create a new entry for it in the roster (necessary to send a new subscription request, as the old source's account will have not just been logged off, but deleted).
                    RosterEntry e;
                    synchronized(roster){
                        if (roster.contains(user)){
                            Presence p = new Presence(Presence.Type.subscribe);
                            p.setTo(user);
                            conn.sendPacket(p);
                        }else
                            roster.createEntry(user,user.substring(0,user.indexOf("@")),groups);
                    }
                    //restart the SourceConfirmer if it isn't already running.
                    start();
                } catch (XMPPException ex) {
                    System.out.println(ex.getMessage());
                }
            }
        }
        
        /**
         * A Thread that loops through every roster entry and, if the entry is unavailable, re-subscribes to the account.
         */
        private class SourceConfirmer extends Thread {
            /**
             * Loops through every roster entry and, if the entry is unavailable, re-subscribes to the account.
             */
            @Override
            public void run(){
                //while there are entries listed as unavailable, and the node is running...
                boolean confirmed = false;
                while (running && !confirmed){
                    RosterGroup srcs;
                    synchronized(roster){
                        srcs = roster.getGroup(SOURCES);
                    }
                    //default confirmed to be true, as unconfirmed entries are detected below
                    confirmed = true;
                    if (srcs != null && (srcs.getEntryCount() > 0)){
                        //for every entry in the roster...
                        for (RosterEntry entry : srcs.getEntries()){
                            String user = entry.getUser();
                            //if the presence of the entry is not "available"
                            if (!roster.getPresence(user).getType().equals(Presence.Type.available)){
                                //set confirmed to false (will loop round again)
                                confirmed = false;
                                //resend subscribe packet
                                synchronized(roster){
                                    Presence p = new Presence(Presence.Type.subscribe);
                                    p.setTo(user);
                                    conn.sendPacket(p);
                                }
                            }
                        }
                    }else confirmed = false;
                    try {
                        //Pause for 5 seconds so as to not saturate the network with requests.
                        Thread.sleep(5000);
                    } catch (InterruptedException ex) {
                        System.out.println(ex.getMessage());
                    }
                }
            }
            
        }
        
    }
    
    /**
     * A class for listening to feeds once they are confirmed.  Each feed has a dedicated InputListener, which adds the data it receives to the feed's stream input buffer.
     */
    private class InputListener implements MessageListener {
        
        private Queue<Triple> in;
        
        /**
         * Creates a new InputListener that will feed received data into the input buffer "stream".
         * @param Queue<Triple> stream - the feed's stream input buffer. 
         */
        public InputListener(Queue<Triple> stream){
            in = stream;
        }

        /**
         * Extract all triples from the body of an incoming message, and place those triples into the input buffer this listener is dedicated to.
         * @param Chat chat - the chat that the message has arrived on
         * @param Message msg - the received message
         */
        public void processMessage(Chat chat, Message msg) {
            List<Triple> triples;
            //Extract as many triples as possible from the received message "msg".
            try {
                triples = DualTimeQuad.listFromString(msg.getBody());
            } catch (RDFTypeException ex) {
                triples = ex.getResults();
            }
            //add all triples to the input buffer "in".
            for (Triple triple : triples){
                synchronized(in){
                    in.offer(triple);
                    //notify threads waiting for the buffer to contain triples.
                    in.notify();
                }
            }
        }
    }
    
    /**
     * A class for handling the receipt of subscription requests from other nodes.
     */
    private class SubscribeListener implements PacketListener {
        /**
         * Creates a new output channel (or "Chat") to the node that the packet is from, and send confirmation messages to it.
         * @param Packet packet - A presence packet received from a subscribing node.
         */
        public void processPacket(Packet packet){
            //retrieve name of subscribing node.
            String user = packet.getFrom();
            //create and send a "subscribed" presence packet in response to the subscription request
            Presence p = new Presence(Presence.Type.subscribed);
            p.setTo(user);
            conn.sendPacket(p);
            //try to create channel and send message
            try {
                //Create channel.
                Chat chat = conn.getChatManager().createChat(user, null);
                //mutex lock based on "listeners" object
                synchronized(listeners){
                    //add chat to list of listening channels.
                    listeners.put(user, chat);
                }
                //create and send an acknowledgement message to the listening node, on the channel that will be used for all data.
                Message message = new Message();
                message.setProperty("acknowledgement","not-null");
                chat.sendMessage(message);
            } catch (XMPPException ex) {
                System.out.println(ex.getMessage());
            } 
        }
    }
    /**
     * A class for filtering in packets that contain presence based subscription requests.
     */
    private class SubscribeFilter implements PacketFilter {
        /**
         * Processes a packet to determine whether it is a presence packet, and whether it is a subscription request.
         * @param Packet packet - any packet received by the node through the connection to the XMPP server.
         * @return boolean - true if the packet matches the filter requirements, false otherwise.
         */
        public boolean accept(Packet packet) {
            return packet instanceof Presence
                && ((Presence)packet).getType().equals(Presence.Type.subscribe);
        }
    }
    
    /**
     * The method used by subclasses for sending data from the node to its listeners.
     * It actually only adds the data to the output buffer, and notifies the Sender thread when the intended packet size has been reached.
     * @param Triple t - an RDF triple that has been passed out of the node for forwarding to the node's listeners.
     */
    protected void sendData(Triple t){
        //Mutex locked method to add triple to "output" list
        sendThread.offer(t);
        //try and print out the fact that data has been "sent" (TODO remove from final version).
        try {
            System.out.println("Node: " + name +"  Data Sent: "+t.toRDF());
        } catch (RDFPartException ex) {
            System.out.println(ex.getMessage());
        } catch (RDFTypeException ex) {
            System.out.println(ex.getMessage());
        }
    }
    
    /**
     * Dictates what format of Triple the node outputs.
     * @param Triple t - The triple to be formatted.
     * @return String formattedTriple - The stringified form of the Triple.
     * @throws RDFPartException
     * @throws RDFTypeException 
     */
    abstract protected String formatTriple(Triple t)  throws RDFPartException, RDFTypeException;
    
    /**
     * Retrieves the next triple to have been received by the node.
     * If the input buffer is currently empty, the thread will block until a triple is received.
     * @param String stream - the name of the stream to take data from.
     * @return Triple next - the next oldest piece of data to have been received on the stream that had not yet been retrieved.
     * @throws InterruptedException 
     */
    protected Triple retrieveData(String stream) throws InterruptedException{
        Queue<Triple> in = input.get(stream);
        synchronized(in){
            while (in.isEmpty())
                in.wait();
            return in.poll();
        }
    }
    
    //Thread Based Operations
    /**
     * Mimics the Thread.start() operation, but in fact triggers multiple threads, all needed to keep the node functioning.
     */
    public void start(){
        //boolean to maintain the loop.
        running = true;
        //initialise the thread that contains the overlaying operation of the node, depending on its type.
        mainThread = new Thread(this);
        //initialise the thread that constantly consumes from the send buffer and actually sends it to the node's listeners.
        sendThread = new PacketSender();
        //start the main thread
        mainThread.start();
        //start the send buffer consumer.
        sendThread.start();
        //start the thread that confirms that the nodes that this node listens to are operating and aware of the this node's subscription
        timeout.start();
    }
    
    /**
     * Provides functionality not present in java threads, allowing an infinitely looping thread to be terminated safely.
     */
    public void stop(){
        running = false;
        boolean waiting = true;
        while (waiting)
            try {
                mainThread.join();
                sendThread.join();
                waiting = false;
            } catch (InterruptedException ex) {}
    }
    
    /**
     * Indicates whether the node is currently active.
     * @return boolean running - indicates whether the node is currently active.
     */
    public boolean isRunning(){
        return running;
    }
    
    /**
     * Contains the call to "loop()", which is the method to be overridden by subclasses.
     * Loops that method until "running" has been set to false by the method "stop()".
     */
    public void run(){
        while (running){
            loop();
        }
    }
    
    /**
     * The method overridden by subclasses, containing the behavior of the specific type of subclass.
     */
    abstract public void loop();
    
    /**
     * A consumer thread for the output buffer of the node, that converts each triple and sends groups of them via XMPP to all listeners.
     */
    private class PacketSender extends Thread {
        
        private Queue<Triple> output;
        
        public PacketSender(){
            output = new ArrayDeque<Triple>();
        }
        
        public synchronized void offer(Triple t){
            //add the triple to the output queue.
            output.offer(t);
            //test for output size exceeding the intended max packet size, and notify the sender if it does.
            if (output.size() > PACKET_SIZE)
                output.notify();
            //System.out.println(output.size());
        }
        /**
         * The mechanics of the thread.
         */
        @Override
        public void run(){
            //loop until "stop()" is called in the parent class.
            while (running){
                //initialise the the variable "msg", to ensure later that data has been added to the output buffer and been converted to a string.
                String msg = "";
                //mutex lock on "output" list.
                synchronized(output){
//                    Wait for the first of two triggers: either the time since the last send exceeds the maximum defined packet age, or the size of the output buffer exceeds the defined packet size.
                    try {
                        output.wait(PACKET_AGE);
                    } catch (InterruptedException ex) {
                        System.out.println(ex.getMessage());
                    }
                    //empty the output buffer and convert all triples to strings, concatonated into the string "msg"
                    Triple triple;
                    while ((triple = output.poll()) != null){
                        try{
                            msg += formatTriple(triple);
                        } catch (RDFPartException ex) {
                            System.out.println(ex.getMessage());
                        } catch (RDFTypeException ex) {
                            System.out.println(ex.getMessage());
                        }
                    }
                }
                try{
                    //if the "msg" string is not empty then obtain the mutex lock on "listeners" list
                    if (!msg.equals("")) synchronized(listeners){
                        //if there are any listeners subscribed...
                        if (!listeners.isEmpty()){
                            //Send "msg" as an XMPP message on all of the registered streams.
                            for (Chat stream : listeners.values()){
                                stream.sendMessage(msg);
                            }
                        }
                    }
                } catch (XMPPException ex) {
                    System.out.println(ex.getMessage());
                }
            }
        }
    }
    
    /**
     * A customised priority queue ("sliding window") that provides functionality to accommodate:
     * * Timestamped data,
     * * Uniqueness within the data (e.g. subject must be unique within the window),
     * * Size or time limiting of the window.
     */
    protected static class WindowQueue extends PriorityQueue {
        
        private static final int SIZE = 100;
        
        private boolean sizeLimited;
        private long limit;
        private boolean subUn,obUn,predUn;
        
        /**
         * Creates a customised sliding window with a fixed starting size and without any unique fields.
         * @param boolean sizeCapped - true if the window is size capped, false if the window is time limited
         * @param long cap - either the max size or max age of the window
         */
        public WindowQueue(boolean sizeCapped,long cap){
            this(SIZE,sizeCapped,cap,false,false,false);
        }
        
        /**
         * Creates a customised sliding window with a customised starting size and without any unique fields.
         * @param int size - the starting size of the window
         * @param boolean sizeCapped - true if the window is size capped, false if the window is time limited
         * @param long cap - either the max size or max age of the window
         */
        public WindowQueue(int size,boolean sizeCapped,long cap){
            this(size,sizeCapped,cap,false,false,false);
        }
        
        /**
         * Creates a customised sliding window with a fixed starting size and customised unique fields.
         * @param boolean sizeCapped - true if the window is size capped, false if the window is time limited
         * @param long cap - either the max size or max age of the window
         * @param subUnique - true if only a single triple with any given subject may be contained in the window
         * @param predUnique - true if only a single triple with any given predicate may be contained in the window
         * @param obUnique - true if only a single triple with any given object may be contained in the window
         */
        public WindowQueue(boolean sizeCapped,long cap,boolean subUnique,boolean predUnique,boolean obUnique){
            this(SIZE,sizeCapped,cap,subUnique,predUnique,obUnique);
        }
        
        /**
         * Creates a customised sliding window with a customised starting size and customised unique fields.
         * @param int size - the starting size of the window
         * @param boolean sizeCapped - true if the window is size capped, false if the window is time limited
         * @param long cap - either the max size or max age of the window
         * @param subUnique - true if only a single triple with any given subject may be contained in the window
         * @param predUnique - true if only a single triple with any given predicate may be contained in the window
         * @param obUnique - true if only a single triple with any given object may be contained in the window
         */
        public WindowQueue(int size,boolean sizeCapped,long cap,boolean subUnique,boolean predUnique,boolean obUnique){
            //set the comparator for the priority queue:
            //the comparisson is the difference between the emmission times of the first and second triples.
            //if those are equal and both tripples are annotated with a second timestamp,
            //then the comparison is the difference between the valid times of the first and second triples.
            super(size,new java.util.Comparator(){
                public int compare(Object o1, Object o2) {
                    if (o1 instanceof UniTime && o2 instanceof UniTime){
                        if (o1 instanceof DualTime && o2 instanceof DualTime && ((UniTime)o1).getTime() == ((UniTime)o2).getTime()){
                            return (int)(((DualTime)o1).getValidTime()
                                       - ((DualTime)o2).getValidTime());
                        }
                        return (int)(((UniTime)o1).getTime()
                                   - ((UniTime)o2).getTime());
                    }
                    return 0;
                }
            });
            //set customised parameters
            sizeLimited = sizeCapped;
            limit = cap;
            subUn = subUnique;
            predUn = predUnique;
            obUn = obUnique;
        }
        
        /**
         * Extends the behavior of the standard PriorityQueue.offer() method.
         * Now wraps untimestamped triples in a triple wrapper with a timestamp, which allows triples to be added and prioritised by the date-time they were added to the window.
         * Also checks for older triples that clash with unique fields in the current triple, and removes them.
         * @param Object ob - the triple to be added to the window (despite being of type Object, the first requirement of "ob" being added to the window is that it is of type Triple).
         * @return boolean success - returns true if the object is successfully added to the window
         */
        @Override
        public boolean offer(Object ob){
            //check if the argument is a triple
            if (ob instanceof Triple){
                Triple t;
                //wrap the triple being offered in a timestamped triple wrapper if it is not already timestamped.
                if (ob instanceof UniTime){
                    t = (Triple) ob;
                }else{
                    t = new TimeStampedTriple((Triple)ob);
                }
                //if the subject is a unique field...
                if (subUn){
                    //for all triples in the window...
                    for (Object obj : this){
                        try {
                            //if the subject of the current old triple is equal to the subject of the offered triple...
                            if (((Triple)obj).getPart(TuplePart.SUBJECT).equals(t.getPart(TuplePart.SUBJECT))){
                                //and the old triple is, in fact, older than the offered triple...
                                if (((UniTime)obj).getTime() < ((UniTime)t).getTime()){
                                    //remove the old triple from the window.
                                    remove(obj);
                                    break;
                                //and if the old triple is younger than the offered triple, drop the offered triple and return false.
                                }else return false;
                            }
                        } catch (RDFPartException ex) {}
                    }
                }
                //The above apples for objects and predicates too.
                if (obUn){
                    for (Object obj : this){
                        try {
                            if (((Triple)obj).getPart(TuplePart.OBJECT).equals(t.getPart(TuplePart.OBJECT))){
                                if (((UniTime)obj).getTime() < ((UniTime)t).getTime()){
                                    remove(obj);
                                    break;
                                }else return false;
                            }
                        } catch (RDFPartException ex) {}
                    }
                }
                if (predUn){
                    for (Object obj : this){
                        try {
                            if (((Triple)obj).getPart(TuplePart.PREDICATE).equals(t.getPart(TuplePart.PREDICATE))){
                                if (((UniTime)obj).getTime() < ((UniTime)t).getTime()){
                                    remove(obj);
                                    break;
                                }else return false;
                            }
                        } catch (RDFPartException ex) {}
                    }
                }
                //if all checks are passed, offer the triple through to the underlying priority queue.
                return super.offer(t);
            }
            return false;
        }
        
        /**
         * Extends the behavior of the standard PriorityQueue.poll() method.
         * Now unwraps timestamp wrappered triples.
         * @return Object ob - the triple removed from the window (despite being of type Object, all objects returned will be triples).
         */
        @Override
        public Object poll(){
            Object ob = super.poll();
            if (ob instanceof TimeStampedTriple) return ((TimeStampedTriple)ob).extract();
            return ob;
        }
        
        /**
         * Provides an means of extending the life of an existing triple in the window.
         * If the triple is identical to one already in the window then the new triple takes the maximum of of the two's emission times (and the minimum of the two's valid times, if they are DualTime instances).
         * the pevious triple is then removed and the new one replaces it
         * @param Triple t - The triple to be added to window
         * @return boolean combined - if the triple has extended the duration of an identical triple already in the window.
         */
        public boolean combine(Triple t){
            Iterator<Triple> it = iterator();
            while (it.hasNext()){
                Triple o = it.next();
                if (t.combine(o)){
                    remove(o);
                    return offer(t);
                }
            }
            return false;
        }
        
        /**
         * Deletes triples that exceed the size limits of the window.
         * @return boolean pruned - returns true if the window has been changed as a result of the call.
         */
        public boolean prune(){
            boolean pruned = false;
            if (sizeLimited){
                while (size() > limit){
                    poll();
                    pruned = true;
                }
            }else{
                long oldest = (new Date()).getTime() - limit;
                while (oldest > ((UniTime)peek()).getTime()){
                    poll();
                    pruned = true;
                }
            }
            return pruned;
        }
        
        /**
         * A wrapper class for triples that are not time annotated, providing time annotation for the purposes of priority queuing.
         */
        private static class TimeStampedTriple extends Triple implements UniTime {

            private long time;
            
            /**
             * Wraps a triple in a UniTime Triple wrapper.
             * @param Triple t - the triple to be wrapped
             */
            public TimeStampedTriple(Triple t){
                super(t.getSubject(),t.getPredicate(),t.getObject());
                time = (new Date()).getTime();
            }
            
            /**
             * Returns the wrapped triple from its wrapper.
             * @return Triple wrapped - the Triple wrapped in this wrapper.
             */
            public Triple extract(){
                return new Triple(getSubject(),getPredicate(),getObject());
            }
            
            /**
             * Returns the time at which this triple was wrapped.
             * @return long time - the time at which this Triple was wrapped
             */
            public long getTime() {
                return time;
            }

            /**
             * Although these methods are never used, they are implemented as part of the UniTime template.
             * Returns an N-triples stringified representation of the timestamp of the Triple.
             * @return String RDFTime - represents the time in N-triples RDF format
             * @throws RDFPartException
             * @throws RDFTypeException 
             */
            public String timeToRDF() throws RDFPartException, RDFTypeException {
                return partToRDF(TuplePart.EMISSION_TIME);
            }

            /**
             * Although these methods are never used, they are implemented as part of the UniTime template.
             * Returns an XML stringified representation of the timestamp of the Triple.
             * @return String RDFTime - represents the time in XML RDF format
             * @throws RDFPartException
             * @throws RDFTypeException 
             */
            public String timeToXMLRDF() throws RDFPartException, RDFTypeException {
                return partToXMLRDF(TuplePart.EMISSION_TIME);
            }
            
        }
        
    }
    
}
