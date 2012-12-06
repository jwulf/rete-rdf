/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package nodes;

import java.sql.Connection;

import java.sql.DriverManager; 
import java.sql.SQLException; 

/**
 *
 * @author Rehab Albeladi
 * @version 1
 */
public class DatabaseSource{
	public static void main(String[] args)     
	{
Connection conn = null;


try { conn =DriverManager.getConnection("jdbc:mysql://152.78.64.186/var/www/iam_sensors/handler" , "raab1g09" , "yazeed84");

System.out.println("connected!");
} catch (SQLException ex) { 
	// handle any errors 
	System.out.println("SQLException: " + ex.getMessage()); 
	System.out.println("SQLState: " + ex.getSQLState()); 
	System.out.println("VendorError: " + ex.getErrorCode());
		}
	}
}
