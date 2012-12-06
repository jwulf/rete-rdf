
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.FileManager;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import nodes.Filter;
import nodes.HTTPSource;
import nodes.Join;
import nodes.RETENode;
import nodes.UDPSource;
import nodes.control.Equal;
import nodes.control.GreaterThan;
import org.jivesoftware.smack.XMPPException;
import rdf.DualTimeQuad;
import rdf.RDFPartException;
import rdf.RDFTypeException;
import rdf.Triple;
import rdf.TuplePart;


/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author David Monks, Rehab Albeladi
 * @version 1
 */
public class Main {
    
    private static final String host = "wsn.ecs.soton.ac.uk",
                            service = "wsn.ecs.soton.ac.uk";
//    private static final String host = "localhost",
//    service = "me.org";
    
    private static final int    port = 5222;

    public static void main(String[] args) throws XMPPException, RDFPartException, RDFTypeException, SocketException, UnknownHostException{
        testFrame();
        testFilter();
        
//        testJoin();
//    	testSSNSource();
//        testSSNNet();
//        testSysLogSource();
//        testUPDSource();        
    }
    
	private static void testSysLogSource() throws XMPPException{
        List<String> list = new ArrayList<String>();
        list.add("syslog@sns.ecs.soton.ac.uk:"+Output.IN);
        Output node = new Output("test",list);
        node.start();
    }
    
    private static void testUPDSource() throws XMPPException, SocketException, UnknownHostException {
        RETENode udpSource = new UDPSource("udpSource",host,service,port,"id","152.78.189.150",12345);
        udpSource.start();
    }
    
    private static void testSSNSource() throws XMPPException{
        RETENode source = new HTTPSource("source",host,service,port,"http://rdf.api.channelcoast.org/observations/cco/westbay/Hs/latest");
        source.start();
//        List<String> list = new ArrayList<String>();
//        list.add("a@"+service+Output.IN);
//        list.add("b@"+service+Output.IN);
//        list.add("c@"+service+Output.IN);
//        list.add("d@"+service+Output.IN);
//        list.add("e@"+service+Output.IN);
//        Output output = new Output("output",list);
//        output.start();
    }
    
    private static void testSSNNet() throws XMPPException, RDFTypeException, RDFPartException{
        RETENode a,b,c,d,e,f,g,h1,i,j,k,l,m, v;
        ArrayList<String> list = new ArrayList<String>();
        list.add("source@"+service+":default");
//        a = new Filter("a",host, service, port,list,TuplePart.PREDICATE,"<http://www.w3.org/2006/time#hasBeginning>",new Equal());
//        a.start();
        //http://www.w3.org/2006/time#inXSDDateTime
        //java -jar dsr-node.jar -n a -h localhost -s sns.ecs.soton.ac.uk -p 5222 -A value -c predicate "==" "http://www.w3.org/2006/time#inXSDDateTime" source@sns.ecs.soton.ac.uk:default
//        b = new Filter("b",host,service,port,list,TuplePart.PREDICATE,"http://purl.oclc.org/NET/ssnx/ssn#observationResultTime",new Equal());
//        b.start();
//        //java -jar dsr-node.jar -n b -h localhost -s sns.ecs.soton.ac.uk -p 5222 -A value -c predicate "==" "http://purl.oclc.org/NET/ssnx/ssn#observationResultTime" source@sns.ecs.soton.ac.uk:default
//        c = new Filter("c",host,service,port,list,TuplePart.PREDICATE,"http://purl.oclc.org/NET/ssnx/ssn#observationResult",new Equal());
//        c.start();
//        //java -jar dsr-node.jar -n c -h localhost -s sns.ecs.soton.ac.uk -p 5222 -A value -c predicate "==" "http://purl.oclc.org/NET/ssnx/ssn#observationResult" source@sns.ecs.soton.ac.uk:default
//        d = new Filter("d",host,service,port,list,TuplePart.PREDICATE,"http://www.semsorgrid4env.eu/ontologies/SsnExtension.owl#hasQuantityValue",new Equal());
//        //http://purl.oclc.org/NET/ssnx/ssn#hasResult
//        d.start();
//        v = new Filter("v",host,service,port,list,TuplePart.PREDICATE,"http://purl.oclc.org/NET/ssnx/ssn#hasValue",new Equal());
//        v.start();
//        //java -jar dsr-node.jar -n d -h localhost -s sns.ecs.soton.ac.uk -p 5222 -A value -c predicate "==" "http://purl.oclc.org/NET/ssnx/ssn#hasResult" source@sns.ecs.soton.ac.uk:default
        e = new Filter("e",host,service,port,list,TuplePart.PREDICATE,"http://purl.oclc.org/NET/ssnx/ssn#observedProperty",new Equal());
        e.start();
        //java -jar dsr-node.jar -n e -h localhost -s sns.ecs.soton.ac.uk -p 5222 -A value -c predicate "==" "http://purl.oclc.org/NET/ssnx/ssn#observedProperty" source@sns.ecs.soton.ac.uk:default
        list = new ArrayList<String>();
        list.add("e@"+service+":default");
        f = new Filter("f",host,service,port,list,TuplePart.OBJECT,"http://marinemetadata.org/2005/08/ndbc_waves#Wind_Wave_Height",new Equal());
        //http://www.semsorgrid4env.eu/ontologies/CoastalDefences.owl#WaveHeight
        f.start();
//        //java -jar dsr-node.jar -n f -h localhost -s sns.ecs.soton.ac.uk -p 5222 -A value -c predicate "==" "http://www.semsorgrid4env.eu/ontologies/CoastalDefences.owl#WaveHeight" e@sns.ecs.soton.ac.uk:default
//        ArrayList<Join.Condition> conds = new ArrayList<Join.Condition>();
//        list = new ArrayList<String>();
////        
//        list.add("a@"+service+":left");
//        list.add("b@"+service+":right");
//        conds.add(new Join.Condition(TuplePart.SUBJECT,new Equal(), TuplePart.OBJECT));
//        m = new Join("m",host,service,port,list,conds,new Join.Template(TuplePart.SUBJECT,Join.RIGHT,null,-1,TuplePart.OBJECT,Join.LEFT));
//        
//        list = new ArrayList<String>();
//        conds = new ArrayList<Join.Condition>();
//        list.add("d@"+service+":left");
//        list.add("v@"+service+":right");
//        conds.add(new Join.Condition(TuplePart.SUBJECT,new Equal(), TuplePart.OBJECT));
//        l = new Join("l",host,service,port,list,conds,new Join.Template(TuplePart.SUBJECT,Join.RIGHT,null,-1,TuplePart.OBJECT,Join.LEFT));
//        
//        list = new ArrayList<String>();
//        conds = new ArrayList<Join.Condition>();
//        list.add("c@"+service+":left");
//        //list.add("f@"+service+":right");
//        list.add("l@"+service+":right");
//        //conds.add(new Join.Condition(TuplePart.SUBJECT,new Equal(), TuplePart.SUBJECT));
//        conds.add(new Join.Condition(TuplePart.OBJECT,new Equal(), TuplePart.SUBJECT));
//        g = new Join("g",host,service,port,list,conds,new Join.Template(TuplePart.SUBJECT,Join.LEFT,null,-1,TuplePart.OBJECT,Join.RIGHT)); //LEFT
//        
//        list = new ArrayList<String>();
//        conds = new ArrayList<Join.Condition>();
//        //list.add("d@"+service+":left");
//        list.add("f@"+service+":left");
//        list.add("g@"+service+":right");
//        conds.add(new Join.Condition(TuplePart.SUBJECT,new Equal(), TuplePart.SUBJECT));
//        h1 = new Join("h1",host,service,port,list,conds,new Join.Template(TuplePart.SUBJECT,Join.RIGHT,null,-1,TuplePart.OBJECT,Join.RIGHT)); //LEFT
//        
//        list = new ArrayList<String>();
//        conds = new ArrayList<Join.Condition>();
//        list.add("m@"+service+":left");
//        list.add("h1@"+service+":right");
//        conds.add(new Join.Condition(TuplePart.SUBJECT,new Equal(), TuplePart.SUBJECT));
//        i = new Join("i",host,service,port,list,conds,new Join.Template(TuplePart.OBJECT,Join.RIGHT,TuplePart.SUBJECT,Join.RIGHT,TuplePart.OBJECT,Join.LEFT));
//        
//        list = new ArrayList<String>();
//        conds = new ArrayList<Join.Condition>();
//        list.add("i@"+service+":left");
//        conds.add(new Join.Condition(TuplePart.SUBJECT,new GreaterThan(), TuplePart.SUBJECT));
//        conds.add(new Join.Condition(TuplePart.OBJECT,new GreaterThan(), TuplePart.OBJECT));
//        //conds.add(new Join.Condition(TuplePart.PREDICATE,new Equal(), TuplePart.PREDICATE));
//        j = new Join("j",host,service,port,list,conds,new Join.Template(TuplePart.OBJECT,Join.LEFT,TuplePart.PREDICATE,Join.LEFT,TuplePart.OBJECT,Join.RIGHT));
//        
////        list = new ArrayList<String>();
////        conds = new ArrayList<Join.Condition>();
////        list.add("j@"+service+":left");
////        conds.add(new Join.Condition(TuplePart.OBJECT,new Equal(), TuplePart.SUBJECT));
////        conds.add(new Join.Condition(TuplePart.PREDICATE,new Equal(), TuplePart.PREDICATE));
////        k = new Join("k",host,service,port,list,conds,new Join.Template(TuplePart.SUBJECT,Join.LEFT,TuplePart.PREDICATE,Join.LEFT,TuplePart.OBJECT,Join.RIGHT));
////        
////        list = new ArrayList<String>();
////        conds = new ArrayList<Join.Condition>();
////        list.add("j@"+service+":left");
////        list.add("k@"+service+":right");
////        conds.add(new Join.Condition(TuplePart.OBJECT,new Equal(), TuplePart.SUBJECT));
////        conds.add(new Join.Condition(TuplePart.PREDICATE,new Equal(), TuplePart.PREDICATE));
////        l = new Join("l",host,service,port,list,conds,new Join.Template(TuplePart.PREDICATE,Join.LEFT,null,-1,TuplePart.SUBJECT,Join.LEFT));
////        
//        m.start();
//        g.start();
//        h1.start();
//        i.start();
//        j.start();
//        l.start();
//        l.start();
    }
    
    private static void testFrame() throws XMPPException{
        String[] nodeData1 = {DualTimeQuad.OPEN_CHAR+Triple.OPEN_CHAR+
                                    "42"+Triple.SEPARATOR+
                                    "0"+Triple.SEPARATOR+
                                    "\"World\""+Triple.SEPARATOR+
                                    "<http://lol>"+Triple.CLOSE_CHAR+
                                Triple.SEPARATOR+
                                "\"2011-07-07T16:50:07\"^^<http://www.w3.org/2001/XMLSchema#datetime>"+Triple.SEPARATOR+
                                "\"2011-07-07T17:00:00\"^^<http://www.w3.org/2001/XMLSchema#datetime>"+DualTimeQuad.CLOSE_CHAR},
                 nodeData2 = {DualTimeQuad.OPEN_CHAR+Triple.OPEN_CHAR+
                                    "\"Hello\""+Triple.SEPARATOR+
                                    "0"+Triple.SEPARATOR+
                                    "42"+Triple.SEPARATOR+
                                    "<http://lol>"+Triple.CLOSE_CHAR+
                                Triple.SEPARATOR+
                                "\"2011-07-07T16:50:07\"^^<http://www.w3.org/2001/XMLSchema#datetime>"+Triple.SEPARATOR+
                                "\"2011-07-07T17:00:00\"^^<http://www.w3.org/2001/XMLSchema#datetime>"+DualTimeQuad.CLOSE_CHAR};
        Input node1 = new Input("node1",nodeData1),
              node2 = new Input("node2",nodeData2);
        
        //List<String> list = new ArrayList<String>();
        //list.add("test@sns.uk:"+Output.IN);
        //Output node3 = new Output("node3",list);

        node1.start();
        node2.start();
        //node3.start();
    }
    
    private static void testJoin() throws XMPPException, RDFPartException{
        List<String> list = new ArrayList<String>();
        list.add("node1@wsn.ecs.soton.ac.uk:"+Join.LEFT_STREAM);
        list.add("node2@wsn.ecs.soton.ac.uk:"+Join.RIGHT_STREAM);
        List<Join.Condition> conds = new ArrayList<Join.Condition>();
        conds.add(new Join.Condition(TuplePart.SUBJECT,new Equal(),TuplePart.OBJECT));
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        }
        RETENode n = new Join("test",
                              host,
                              service,
                              port,
                              list,
                              conds,
                              new Join.Template(TuplePart.SUBJECT,Join.RIGHT,
                                                null,-1,
                                                TuplePart.OBJECT,Join.LEFT)
                              );
        n.start();
    }
    
    private static void testFilter() throws XMPPException, RDFPartException, RDFTypeException{
        List<String> list = new ArrayList<String>();
        list.add("node1@wsn.ecs.soton.ac.uk:"+Filter.IN_STREAM);
        list.add("node2@wsn.ecs.soton.ac.uk:"+Filter.IN_STREAM);
        //List<Join.Condition> conds = new ArrayList<Join.Condition>();
        //conds.add(new Join.Condition(TuplePart.SUBJECT,new Equal(),TuplePart.OBJECT));
        RETENode n = new Filter("filter1",
                                host,
                                service,
                                port,
                                list,
                                TuplePart.OBJECT,
                                "\"World\"",
                                new Equal());
        n.start();
        
        list = new ArrayList<String>();
        list.add("filter1@wsn.ecs.soton.ac.uk:"+Filter.IN_STREAM);
        RETENode n1 = new Filter("filter2",
                                host,
                                service,
                                port,
                                list,
                                TuplePart.SUBJECT,
                                "\"42\"",
                                new Equal());
        n1.start();
    }
    
    private static class Input extends RETENode {

        private String[] data;
        private String source;
        private int date, time;
       
        public Input(String name, String[] d) throws XMPPException{
            super(name,host,service,port);
            data = d;
            source = null;
            date = 0;
            time = 0;
        }
        
        public Input(String name, String s) throws XMPPException{
            super(name,host,service,port);
            data = null;
            source = s;
            date = 0;
            time = 0;
        }
       
        @Override
        protected String formatTriple(Triple t) throws RDFPartException, RDFTypeException {
            return t.toRDF();
        }
 
        @Override
        public void loop() {
            if (data != null)
                try {
                    sendData(DualTimeQuad.annotatedQuadFromString(data[(new Random()).nextInt()%data.length]));
                    Thread.sleep(1000);
                    
                } catch (InterruptedException ex) {
                    System.out.println("AAAHHHHH, INTERRUPTION!");
                } catch (RDFPartException ex) {
                    System.out.println("Uh oh, data send error with RDFPart.");
                } catch (RDFTypeException ex) {
                    System.out.println("Uh oh, data send error with RDFType.");
                }
            else{
                final Model model = ModelFactory.createDefaultModel();
                model.read(source);
                //model.write(System.out,"N3");
                StmtIterator iter = model.listStatements();
                String subject;
                if (iter.hasNext() && (subject = iter.nextStatement().getSubject().toString()).startsWith("http://")){
                    int d = Integer.parseInt(subject.substring(subject.lastIndexOf("/")+1,subject.indexOf("#")));
                    int t = Integer.parseInt(subject.substring(subject.indexOf("#")+1));
//                    if (d > date || (d == date && t > time))
                        while (iter.hasNext()){
                            Statement stmt = iter.nextStatement();
                            //System.out.println(stmt.toString());
                            try {
                                sendData(Triple.tripleFromString(stmt.toString()));
                                Thread.sleep(5000);
                            } catch (RDFTypeException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            } catch (RDFPartException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            } catch (InterruptedException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        }
                    date = d;
                    time = t;
                }
           }
       }
        
    }
    
    private static class Output extends RETENode {

        private static final String IN = "default";
        
        public Output(String name,List<String> srcs) throws XMPPException{
            super(name,host,service,port);
            addStream(IN,srcs);
        }
        
        @Override
        protected String formatTriple(Triple t) throws RDFPartException, RDFTypeException {
            return t.toRDF();
        }

        @Override
        public void loop() {
            Triple t;
            try {
                if ((t = retrieveData(IN)) != null){
                    try {
                        System.out.println("Received: "+t.toRDF());
                    } catch (RDFPartException ex) {
                        System.out.println("AAAAHHHH RDFPart");
                    } catch (RDFTypeException ex) {
                        System.out.println("AAAAHHHH RDFType");
                    }
                }
            } catch (InterruptedException ex) {
                System.out.println(ex.getMessage());
            }
        }
        
    }
    
}