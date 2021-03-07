package com.RUStore;

/* any necessary Java packages here */
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.*;

public class RUStoreClient {

	/* any necessary class members here */

	Socket clientSocket;
	private String h; // host ip
	private int p; // port
	//private PrintWriter out ;
	private DataOutputStream out ;
	//private DataOutputStream dOut ;
	private BufferedReader in ;
	//private static DataInputStream dIn ;


	/**
	 * RUStoreClient Constructor, initializes default values for class members
	 *
	 * @param host host url
	 * @param port port number
	 */
	public RUStoreClient(String host, int port) {

		// Implement here

		h = host;
		p = port;

	}

	/**
	 * Opens a socket and establish a connection to the object store server
	 * running on a given host and port.
	 *
	 * @return n/a, however throw an exception if any issues occur
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	public void connect() throws UnknownHostException, IOException {

		// Implement here

		clientSocket = new Socket(h, p);
		//out = new PrintWriter(clientSocket.getOutputStream(), true) ;
		out = new DataOutputStream(clientSocket.getOutputStream()) ;
		//dOut = new DataOutputStream(new BufferedOutputStream(clientSocket.getOutputStream())) ;
		in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream())) ;
		//dIn = new DataInputStream(new BufferedInputStream(clientSocket.getInputStream())) ;

		System.out.println("Connected!");

	}

	/**
	 * Sends an arbitrary data object to the object store server. If an object with
	 * the same key already exists, the object should NOT be overwritten
	 * 
	 * @param key  key to be used as the unique identifier for the object
	 * @param data byte array representing arbitrary data object
	 * 
	 * @return 0 upon success 1 if key already exists
	 * @throws IOException
	 */
	public int put(String key, byte[] data) throws IOException {

		// Implement here
		System.out.println("writing put data");
		out.writeBytes("PUT DATA\n") ;

		System.out.println("wrote, waiting...");

		String response = in.readLine() ;
		System.out.println("response: " + response);

		if (response.equals("KEY?")) {

			out.writeBytes(key + "\n") ;

			response = in.readLine() ;

			if (response.equals("EXISTS")) {

				System.out.println("Key already exists") ;
				return 1 ;

			} else if (response.equals("OPEN")) {

				System.out.println("Key does not exist") ;
				//System.out.println("data length = " + data.length) ;
				//out.println(data.length) ;
				//out.println(new String(data)) ;
				out.writeBytes(new String(data) + "\n") ;


			} else { // Invalid response

				return -1 ;

			}
		
		} else { // Invalid response

			return -1 ;

		}

		return 0 ;

	}

	/**
	 * Sends an arbitrary data object to the object store server. If an object with
	 * the same key already exists, the object should NOT be overwritten.
	 * 
	 * @param key       key to be used as the unique identifier for the object
	 * @param file_path path of file data to transfer
	 * 
	 * @return 0 upon success 1 if key already exists Throw an exception otherwise
	 * @throws IOException
	 */
	public int put(String key, String file_path) throws IOException {

		// Implement here
		System.out.println("writing put data");
		out.writeBytes("PUT FILE\n") ;

		System.out.println("wrote, waiting...");

		String response = in.readLine() ;
		System.out.println("response: " + response);

		if (response.equals("KEY?")) {

			out.writeBytes(key + "\n") ;

			response = in.readLine() ;

			if (response.equals("EXISTS")) {

				System.out.println("Key already exists") ;
				return 1 ;

			} else if (response.equals("OPEN")) {

				System.out.println("Key does not exist") ;
				//System.out.println("data length = " + data.length) ;
				//byte[] data = Files.readAllBytes(Paths.get(file_path)) ;
				//out.print() ;
				//out.println(new String(data)) ;

				//int count ;

				File file = new File(file_path) ;

				out.writeBytes((int) file.length() + "\n");

				byte[] data = new byte[(int) file.length()] ;

				BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file)) ;

				int count ;

				System.out.println("Made it here length = " + file.length());

				while ((count = bis.read(data)) > 0) {

					out.write(data, 0, count) ;
					System.out.println("count = " + count);

				}

				out.writeBytes("\n") ;

				System.out.println("Done\n");

				bis.close() ;

				/*

				FileInputStream fis = new FileInputStream(file) ;
				BufferedInputStream bis = new BufferedInputStream(fis) ;

				bis.read(data, 0, data.length) ;

				clientSocket.getOutputStream().write(data, 0, data.length) ;

				out.flush() ;

				bis.close() ;

				System.out.println("Done\n");

				*/
				
				//out.writeBytes("DONE\n") ;
				
				//File f = new File(file_path) ;
				//out.println(f.getName());
				//out.println(f.length()) ;

				// FileInputStream fis = new FileInputStream(f) ;

				// byte[] buf = new byte[1024] ;
				// int pos = 0 ;
				// int r ;

				// while ((r = fis.read(buf, 0, 1024)) >= 0 ) {

				// 	clientSocket.getOutputStream().write(buf, 0, r);
				// 	clientSocket.getOutputStream().flush() ;
				// 	pos += r ;

				// }

				// fis.close() ;
			

			} else { // Invalid response

				return -1 ;

			}
		
		} else { // Invalid response

			return -1 ;

		}

		return 0 ;

	}

	/**
	 * Downloads arbitrary data object associated with a given key from the object
	 * store server.
	 * 
	 * @param key key associated with the object
	 * 
	 * @return object data as a byte array, null if key doesn't exist. Throw an
	 *         exception if any other issues occur.
	 * @throws IOException
	 */
	public byte[] get(String key) throws IOException {

		// Implement here

		System.out.println("writing get data");
		out.writeBytes("GET DATA\n") ;

		System.out.println("wrote, waiting...");

		String response = in.readLine() ;
		System.out.println("response: " + response);

		if (response.equals("KEY?")) {

			out.writeBytes(key + "\n") ;

			response = in.readLine() ;

			if (response.equals("FOUND")) {

				System.out.println("Key exists") ;
				String data = in.readLine() ;
				return data.getBytes() ;
				//return 1 ;

			} else if (response.equals("ABSENT")) {

				System.out.println("Key not found");
				return null ;

			}

		}

		return null;

	}

	/**
	 * Downloads arbitrary data object associated with a given key from the object
	 * store server and places it in a file.
	 * 
	 * @param key       key associated with the object
	 * @param file_path output file path
	 * 
	 * @return 0 upon success 1 if key doesn't exist Throw an exception otherwise
	 * @throws IOException
	 */
	public int get(String key, String file_path) throws IOException {

		// Implement here

		System.out.println("writing get data");
		out.writeBytes("GET DATA\n") ;

		System.out.println("wrote, waiting...");

		String response = in.readLine() ;
		System.out.println("response: " + response);

		if (response.equals("KEY?")) {

			out.writeBytes(key + "\n") ;

			response = in.readLine() ;

			if (response.equals("FOUND")) {

				System.out.println("Key exists") ;
				int length = in.read() ;

				byte[] fullBytes = new byte[length] ;

				FileOutputStream fos = new FileOutputStream(file_path) ;
				BufferedOutputStream bos = new BufferedOutputStream(fos) ;

				int bytesRead = clientSocket.getInputStream().read(fullBytes, 0, length) ;
				int current = bytesRead ;

				while (bytesRead > -1) {

					bytesRead = clientSocket.getInputStream().read(fullBytes, current, length - current) ;

					if (bytesRead >= 0 ) {

						current += bytesRead ;

					}

				}

				bos.write(fullBytes, 0, current) ;

				bos.flush() ;
				bos.close() ;

			} else if (response.equals("ABSENT")) {

				System.out.println("Key not found");
				return 1 ;

			}

		}

		return 0;

	}

	/**
	 * Removes data object associated with a given key
	 * from the object store server. Note: No need to download the data object,
	 * simply invoke the object store server to remove object on server side
	 * 
	 * @param key key associated with the object
	 * 
	 * @return		0 upon success
	 *        		1 if key doesn't exist
	 *        		Throw an exception otherwise
	 */
	public int remove(String key) {

		// Implement here
		return -1;

	}

	/**
	 * Retrieves of list of object keys from the object store server
	 * 
	 * @return		List of keys as string array, null if there are no keys.
	 *        		Throw an exception if any other issues occur.
	 */
	public String[] list() {

		// Implement here
		return null;

	}

	/**
	 * Signals to server to close connection before closes
	 * the client socket.
	 * 
	 * @return n/a, however throw an exception if any issues occur
	 * @throws IOException
	 */
	public void disconnect() throws IOException {

		// Implement here

		out.writeBytes("DISCONNECT\n") ;

		out.close() ;
		in.close() ;
		clientSocket.close();

		System.out.println("Disconnected!");

	}

}
