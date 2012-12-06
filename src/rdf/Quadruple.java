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
public interface Quadruple {
    
    public Triple.Pair<Object,RDFType> getContext();
    
}
