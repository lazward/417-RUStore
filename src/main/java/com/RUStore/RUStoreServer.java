package com.RUStore;

/* any necessary Java packages here */
import java.net.*;
import java.io.*;
import java.util.* ;

public class RUStoreServer {

	/* any necessary class members here */

	private static ServerSocket serverSocket;
	private static Socket clientSocket;
	private static PrintWriter out ;
	private static BufferedReader in ;
	private static DataInputStream dIn ;
	private static Hashtable<String, Byte[]> data = new Hashtable<String, Byte[]>() ;

	/* any necessary helper methods here */

	/**
	 * RUObjectServer Main(). Note: Accepts one argument -> port number
	 * 
	 * @throws IOException
	 */
	public static void main(String args[]) throws IOException {

		// Check if at least one argument that is potentially a port number
		if (args.length != 1) {
			System.out.println("Invalid number of arguments. You must provide a port number.");
			return;
		}

		// Try and parse port # from argument
		int port = Integer.parseInt(args[0]);

		// Implement here //

		serverSocket = new ServerSocket(port);
		clientSocket = serverSocket.accept();

		out = new PrintWriter(clientSocket.getOutputStream(), true) ;
		in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream())) ;
		dIn = new DataInputStream(clientSocket.getInputStream()) ;

		System.out.println("Client connected!");

		clientSocket.close();

		String inputLine ;

		while ((inputLine = in.readLine()) != null) {

			out.println(inputLine) ;
			switch(inputLine) {

				case "PUT DATA":
					out.println("KEY?");

					String key = in.readLine() ;

					if (data.containsKey(key)) {

						out.println("EXISTS") ;
						break ;

					}

					int length = dIn.readInt() ;

					if (length > 0) {

						byte[] data = new byte[length] ;
						dIn.readFully(data, 0, length) ;

					}

					break ;
				case "PUT FILE":

					break ;

			}

		}

		System.out.println("Client disconnected!");
		
		serverSocket.close();

		System.out.println("Server closed!") ;


	}

}
