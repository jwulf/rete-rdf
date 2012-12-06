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
public class RDFPartException extends Exception{

    public RDFPartException(){
        super("Problem with Tuple Part reference.");
    }
    
    public RDFPartException(String message){
        super(message);
    }
    
}
