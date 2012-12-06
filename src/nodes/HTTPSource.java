/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package nodes;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import org.jivesoftware.smack.XMPPException;

import rdf.RDFPartException;
import rdf.RDFTypeException;
import rdf.Triple;

/**
 *
 * @author David Monks, Rehab Albeladi
 * @version 1
 */
public class HTTPSource extends RETENode{

    private String source;
    private long date,time;
    
    public HTTPSource(String name,
                      String host,
                      String service,
                      int port,
                      String uri) throws XMPPException{
        super(name,host,service,port);
        source = uri;
        date = 0;
        time = 0;
    }
    
    @Override
    protected String formatTriple(Triple t) throws RDFPartException, RDFTypeException {
        return t.toRDF();
    }

    @Override
    public void loop() {
        final Model model = ModelFactory.createDefaultModel();
        model.read(source);
        //model.write(System.out,"N3");
        StmtIterator iter = model.listStatements();
        String subject;
        if (iter.hasNext() && (subject = iter.nextStatement().getSubject().toString()).startsWith("http://")){
            //int d = Integer.parseInt(subject.substring(subject.lastIndexOf("/")+1,subject.indexOf("#")));
            //int t = Integer.parseInt(subject.substring(subject.indexOf("#")+1));
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
//            }
            //date = d;
            //time = t;
        }
    }

    
    
}
