/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package rdf;

/**
 *
 * @author David Monks
 * @version 1
 */
import java.util.ArrayList;
import java.util.List;

public class RDFTypeException extends Exception{
    
    private List<Triple> results;

    public RDFTypeException(){
        super("Unrecognised RDF syntax.");
        results = new ArrayList<Triple>();
    }
    
    public RDFTypeException(String message){
        super(message);
        results = new ArrayList<Triple>();
    }
    
    public RDFTypeException(List<Triple> items){
        super("Unrecognised RDF syntax.");
        results = items;
    }
    
    public RDFTypeException(String message, List<Triple> items){
        super(message);
        results = items;
    }
    
    public List<Triple> getResults(){
        List<Triple> out = new ArrayList<Triple>();
        out.addAll(results);
        return out;
    }
    
    public boolean hasResults(){
        return results.isEmpty();
    }
    
}
