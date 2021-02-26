package com.RUStore;

/* any necessary Java packages here */
import java.net.*;
import java.nio.charset.Charset;
import java.io.*;
import java.util.* ;

public class RUStoreServer {

	/* any necessary class members here */

	private static ServerSocket serverSocket;
	private static Socket clientSocket;
	private static PrintWriter out ;
	//private static DataOutputStream dOut ;
	private static BufferedReader in ;
	//private static DataInputStream dIn ;
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
		//dOut = new DataOutputStream(clientSocket.getOutputStream()) ;
		in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream())) ;
		//dIn = new DataInputStream(clientSocket.getInputStream()) ;

		System.out.println("Client connected!");

		String inputLine ;

		while (((inputLine = in.readLine()) != null) && (!inputLine.equals("DISCONNECT"))) {

			System.out.println("selection: " + inputLine);
			
			switch(inputLine) {

				case "PUT DATA": {

					out.println("KEY?");
					String key = in.readLine() ;
					System.out.println("Key = " + key) ;

					if (data.containsKey(key)) {

						out.println("EXISTS") ;
						System.out.println("Key already exists");
						// break ;

					} else {

						out.println("OPEN") ;
	
							byte[] input = in.readLine().getBytes() ;
	
							System.out.println("input length = " + input.length) ;
	
							//dIn.readFully(input, 0, size) ;
	
							Byte[] bytes = new Byte[input.length] ;
	
							for (int i = 0 ; i < bytes.length ; i++) {
	
								bytes[i] = input[i] ;
	
							}
	
							data.put(key, bytes) ;
	
						

					}


					break ;
					
				}
				case "GET DATA": {

					out.println("KEY?");
					String key = in.readLine() ;
					System.out.println("Key = " + key) ;

					if (data.containsKey(key)) {

						out.println("FOUND") ;
						System.out.println("Key found");

						Byte[] output = data.get(key) ;

						byte[] bytes = new byte[output.length] ;
	
						for (int i = 0 ; i < output.length ; i++) {

							bytes[i] = output[i] ;

						}

						out.println(new String(bytes));
						

					} else {

						out.println("ABSENT") ;
						System.out.println("Not found");

					}

					break ;


				}

			}

		}

		clientSocket.close();

		System.out.println("Client disconnected!");
		
		serverSocket.close();

		System.out.println("Server closed!") ;


	}

}
