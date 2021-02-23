package com.RUStore;

/* any necessary Java packages here */
import java.net.*;
import java.io.*;

public class RUStoreServer {

	/* any necessary class members here */

	private static ServerSocket serverSocket;
	private static Socket clientSocket;

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

		System.out.println("Client connected!");

		clientSocket.close();

		System.out.println("Client disconnected!");
		
		serverSocket.close();

		System.out.println("Server closed!") ;


	}

}
