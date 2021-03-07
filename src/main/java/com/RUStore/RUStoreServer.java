package com.RUStore;

/* any necessary Java packages here */
import java.net.*;
import java.io.*;
import java.util.* ;

public class RUStoreServer {

	/* any necessary class members here */

	private static ServerSocket serverSocket;
	private static Socket clientSocket;
	//private static PrintWriter out ;
	private static DataOutputStream out ;
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

		out = new DataOutputStream(clientSocket.getOutputStream()) ;
		//dOut = new DataOutputStream(clientSocket.getOutputStream()) ;
		in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream())) ;
		//dIn = new DataInputStream(clientSocket.getInputStream()) ;

		System.out.println("Client connected!");

		String inputLine ;

		while (((inputLine = in.readLine()) != null) && (!inputLine.equals("DISCONNECT"))) {

			System.out.println("selection: " + inputLine);
			
			switch(inputLine) {

				case "PUT DATA": {

					out.writeBytes("KEY?\n");
					String key = in.readLine() ;
					System.out.println("Key = " + key) ;

					if (data.containsKey(key)) {

						out.writeBytes("EXISTS\n") ;
						System.out.println("Key already exists");
						// break ;

					} else {

						out.writeBytes("OPEN\n") ;
	
							byte[] input = in.readLine().getBytes() ;
	
							System.out.println("input length = " + input.length) ;
	
							//dIn.readFully(input, 0, size) ;
	
							Byte[] bytes = new Byte[input.length] ;
	
							for (int i = 0 ; i < bytes.length ; i++) {
	
								bytes[i] = input[i] ;
	
							}
	
							data.put(key, bytes) ;

							System.out.println("PUT DATA done") ;
	
					
					}


					break ;
					
				}
				case "GET DATA": {

					out.writeBytes("KEY?\n");
					String key = in.readLine() ;
					System.out.println("Key = " + key) ;

					if (data.containsKey(key)) {

						out.writeBytes("FOUND\n") ;
						System.out.println("Key found");

						Byte[] output = data.get(key) ;

						byte[] bytes = new byte[output.length] ;
	
						for (int i = 0 ; i < output.length ; i++) {

							bytes[i] = output[i] ;

						}

						out.writeBytes(new String(bytes) + "\n");
						

					} else {

						out.writeBytes("ABSENT\n") ;
						System.out.println("Not found");

					}

					break ;

				}

				case "PUT FILE": {

					out.writeBytes("KEY?\n");
					String key = in.readLine() ;
					System.out.println("Key = " + key) ;

					if (data.containsKey(key)) {

						out.writeBytes("EXISTS") ;
						System.out.println("Key already exists");
						// break ;

					} else {

						out.writeBytes("OPEN\n") ;
	
						int length = Integer.parseInt(in.readLine()) ;

						System.out.println("input length = " + length) ;

						byte[] fullBytes = new byte[length] ;

						int count = 0 ;

						System.out.println("Made it here\n");

						ByteArrayOutputStream baos = new ByteArrayOutputStream() ;

						while ((count = clientSocket.getInputStream().read(fullBytes)) > 1) {
							
							baos.write(fullBytes, 0, count) ;
							//out.write(fullBytes, 0, count) ;
							System.out.println("count = " + count);

						}

						
						/*

						int bytesRead = clientSocket.getInputStream().read(fullBytes, 0, length) ;
						int current = bytesRead ;

						while (bytesRead > -1) {

							bytesRead = clientSocket.getInputStream().read(fullBytes, current, length - current) ;

							if (bytesRead >= 0 ) {

								current += bytesRead ;

							}

						}

						*/

						System.out.println("Finished processing file") ;
	
							//dIn.readFully(input, 0, size) ;
	
							Byte[] bytes = new Byte[fullBytes.length] ;
	
							for (int i = 0 ; i < bytes.length ; i++) {
	
								bytes[i] = fullBytes[i] ;
	
							}
	
							data.put(key, bytes) ;

						

					}

					break ;

				}

				case "GET FILE": {

					out.writeBytes("KEY?\n");
					String key = in.readLine() ;
					System.out.println("Key = " + key) ;

					if (data.containsKey(key)) {

						out.writeBytes("FOUND\n") ;
						System.out.println("Key found");

						Byte[] output = data.get(key) ;

						byte[] bytes = new byte[output.length] ;
	
						for (int i = 0 ; i < output.length ; i++) {

							bytes[i] = output[i] ;

						}

						out.write(output.length) ;

						clientSocket.getOutputStream().write(bytes, 0, output.length) ;
						

					} else {

						out.writeBytes("ABSENT\n") ;
						System.out.println("Not found");

					}

					break ;

				}

			}

		}

		out.close() ;
		in.close() ;

		clientSocket.close();

		System.out.println("Client disconnected!");
		
		serverSocket.close();

		System.out.println("Server closed!") ;


	}

}
