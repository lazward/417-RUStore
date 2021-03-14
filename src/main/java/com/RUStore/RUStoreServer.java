package com.RUStore;

/* any necessary Java packages here */
import java.net.*;
import java.io.*;
import java.util.* ;

public class RUStoreServer {

	/* any necessary class members here */

	private static ServerSocket serverSocket;
	private static Socket clientSocket;
	private static DataOutputStream out ;
	private static BufferedReader in ;
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
			//System.out.println("Invalid number of arguments. You must provide a port number.");
			return;
		}

		// Try and parse port # from argument
		int port = Integer.parseInt(args[0]);

		// Implement here //

		serverSocket = new ServerSocket(port);

		while (true) {

			clientSocket = serverSocket.accept();

			out = new DataOutputStream(clientSocket.getOutputStream()) ;
			in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream())) ;
	
			//System.out.println("Client connected!");
	
			String inputLine ;
	
			while (((inputLine = in.readLine()) != null) && (!inputLine.equals("DISCONNECT"))) {
	
				//System.out.println("selection: " + inputLine);
				
				switch(inputLine) {
	
					case "PUT DATA": {
	
						out.writeBytes("KEY?\n");
						String key = in.readLine() ;
						//System.out.println("Key = " + key) ;
	
						if (data.containsKey(key)) {
	
							out.writeBytes("EXISTS\n") ;
							//System.out.println("Key already exists");
							// break ;
	
						} else {
	
							out.writeBytes("OPEN\n") ;
		
								byte[] input = in.readLine().getBytes() ;
		
								//System.out.println("input length = " + input.length) ;
		
								Byte[] bytes = new Byte[input.length] ;
		
								for (int i = 0 ; i < bytes.length ; i++) {
		
									bytes[i] = input[i] ;
		
								}
		
								data.put(key, bytes) ;
	
								//System.out.println("PUT DATA done") ;
		
						
						}
	
	
						break ;
						
					}
					case "GET DATA": {
	
						out.writeBytes("KEY?\n");
						String key = in.readLine() ;
						//System.out.println("Key = " + key) ;
	
						if (data.containsKey(key)) {
	
							out.writeBytes("FOUND\n") ;
							//System.out.println("Key found");
	
							Byte[] output = data.get(key) ;
	
							byte[] bytes = new byte[output.length] ;
		
							for (int i = 0 ; i < output.length ; i++) {
	
								bytes[i] = output[i] ;
	
							}
	
							out.writeBytes(new String(bytes) + "\n");
							
	
						} else {
	
							out.writeBytes("ABSENT\n") ;
							//System.out.println("Not found");
	
						}
	
						break ;
	
					}
	
					case "PUT FILE": {
	
						out.writeBytes("KEY?\n");
						String key = in.readLine() ;
						//System.out.println("Key = " + key) ;
	
						if (data.containsKey(key)) {
	
							out.writeBytes("EXISTS") ;
							//System.out.println("Key already exists");
	
						} else {
	
							out.writeBytes("OPEN\n") ;
		
							int length = Integer.parseInt(in.readLine()) ;
	
							//System.out.println("input length = " + length) ;
	
							byte[] fullBytes = new byte[length] ;
	
							int count = 0 ;
	
							//System.out.println("Made it here\n");
	
							ByteArrayOutputStream baos = new ByteArrayOutputStream() ;
	
							int bytesRead = 0 ;
	
							byte[] buffer = new byte[length] ;
	
							//FileOutputStream fos = new FileOutputStream("./outputfiles/idk.jpg") ;
							//BufferedOutputStream bos = new BufferedOutputStream(fos) ;
	
							while ((bytesRead < length)) {
								
								count = clientSocket.getInputStream().read(buffer, bytesRead, (length - bytesRead)) ;
								//bos.write(fullBytes, 0, count) ;
								//out.write(fullBytes, 0, count) ;
								bytesRead += count ;
								//System.out.println("count = " + count + " bytesRead = " + bytesRead);
	
							}

							baos.write(buffer) ;
	
							//System.out.println("done");
	
							fullBytes = baos.toByteArray() ;
	
							//fos.write(fullBytes) ;
							//baos.flush(); 
							baos.close();
							//bos.close() ;
							//fos.close() ;
	
							//System.out.println("Finished processing file") ;
	
							out.writeBytes("DONE\n");
		
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
						//System.out.println("Key = " + key) ;
	
						if (data.containsKey(key)) {
	
							out.writeBytes("FOUND\n") ;
							//System.out.println("Key found");
	
							Byte[] output = data.get(key) ;
	
							byte[] bytes = new byte[output.length] ;
		
							for (int i = 0 ; i < output.length ; i++) {
	
								bytes[i] = output[i] ;
	
							}
	
							//FileOutputStream fos = new FileOutputStream("./outputfiles/test2_.jpg") ;
							//fos.write(bytes);
							//fos.close();
	
							out.writeBytes((int) output.length + "\n") ;
	
							//System.out.println("length = " + bytes.length);
	 
							ByteArrayInputStream bais = new ByteArrayInputStream(bytes) ;
	
							int count ;
	
							int bytesRead = 0 ;
	
							byte[] buffer = new byte[bytes.length] ;
	
							while ((bytesRead < bytes.length) && ((count = bais.read(buffer)) > 0)) {
	
								out.write(buffer, 0, count) ;
								bytesRead += count ;
	
							}
	
							out.flush();
	
							//System.out.println("Done");
							
						} else {
	
							out.writeBytes("ABSENT\n") ;
							//System.out.println("Not found");
	
						}
	
						break ;
	
					}
	
					case "REMOVE": {
	
						out.writeBytes("KEY?\n");
						String key = in.readLine() ;
	
						if (data.containsKey(key)) {
	
							out.writeBytes("FOUND\n");
							data.remove(key) ;
	
						} else {
	
							out.writeBytes("ABSENT\n");
	
						}
	
						break ;
	
					}
	
					case "LIST": {
	
						out.writeBytes(data.size() + "\n");
	
						if (data.size() == 0) {
	
							break ;
	
						} else {
	
							for (String key : data.keySet()) {
	
								out.writeBytes(key + "\n");
		
							}
	
						}
	
						break ;
	
					}
	
				}
	
			}
	
			out.close() ;
			in.close() ;
	
			clientSocket.close();
	
			//System.out.println("Client disconnected!");

		}
	
		
		//serverSocket.close();

		////System.out.println("Server closed!") ;


	}

}
