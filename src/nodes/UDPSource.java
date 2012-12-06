package nodes;

import java.io.IOException;

import java.net.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jivesoftware.smack.XMPPException;

import org.json.JSONException;
import org.json.JSONObject;

import rdf.RDFPartException;
import rdf.RDFTypeException;
import rdf.Triple;

public class UDPSource extends RETENode {

    private DatagramSocket socket;
    private int port;
    private InetAddress server;
    
    private String subjectTag;
    
    public UDPSource(String name,
                   String host,
                   String service,
                   int pXMPP,
                   String subtg,
                   String srvr,
                   int pUDP) throws XMPPException, SocketException, UnknownHostException{
        super(name,host,service,pXMPP);
        socket = new DatagramSocket();
        port = pUDP;
        server = InetAddress.getByName(srvr);
        subjectTag = subtg;
    }
    
    @Override
    public void start(){
        super.start();
        byte[] sendData = "Hello".getBytes();
        try {
            socket.send(new DatagramPacket(sendData, sendData.length, server, port));
            
        } catch (IOException ex) {}
    }
    
    @Override
    public void stop(){
        super.stop();
        socket.close();
    }

    @Override
    protected String formatTriple(Triple t) throws RDFPartException, RDFTypeException {
        return t.toRDF();
    }

    @Override
    public void loop() {
        if (socket.isBound()) try {
        	
            byte[] receiveData = new byte[1024];
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            socket.receive(receivePacket);
            String data = new String(receivePacket.getData());
            //System.out.println(data);
            JSONObject object = new JSONObject(data);
            String subject = object.getString(subjectTag);
            Iterator<String> it = object.keys();
            for (String pred = it.next(); it.hasNext(); pred = it.next()){
                if (!pred.equals(subjectTag)){
                    try {
                        sendData(new Triple(subject,"\""+pred+"\"","\""+object.optString(pred)+"\""));
                    } catch (RDFTypeException ex) {
                        System.out.println(ex.getMessage());
                    }
                }
            }
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        } catch (JSONException ex) {
            System.out.println(ex.getMessage());
        }
    }
}


