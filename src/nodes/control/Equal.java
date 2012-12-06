/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package nodes.control;

/**
 *
 * @author David Monks, dm11g08
 */
public class Equal extends Equality {

    @Override
    public boolean compare(Object a, Object b){
        return a.equals(b);
    }
    
}
