package nodes;

import java.io.*;
import java.net.*;
	
public class UDPTest {
	

		public static void main(String args[]) throws Exception {

			DatagramSocket clientSocket = new DatagramSocket();
			InetAddress ip = InetAddress.getByName("152.78.189.150");
			int port = 12345;

			//  say hello to the server
			byte[] sendData = "Hello".getBytes();
			clientSocket.send(new DatagramPacket(sendData, sendData.length, ip, port));

			//  receive all data we can get :)
			while (true) {	
				byte[] receiveData = new byte[1024];
				DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
				clientSocket.receive(receivePacket);
				String data = new String(receivePacket.getData());
				System.out.println("DATA:" + data);
			}

			//clientSocket.close();
		}
	

}
