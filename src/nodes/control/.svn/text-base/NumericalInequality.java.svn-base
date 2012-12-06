/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package nodes.control;

/**
 *
 * @author David Monks, dm11g08
 * @version 1
 */
abstract public class NumericalInequality extends Comparator {

    @Override
    public boolean compare(Object a,
                           Object b) {
        double x,y;
        x = ((Double) a).doubleValue();
        y = ((Double) b).doubleValue();
        return compare(x,y);
    }
    
    abstract protected boolean compare(double a,
                                       double b);

}
