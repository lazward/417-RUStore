package com.RUStore;

/* any necessary Java packages here */
import java.net.*;
import java.nio.charset.Charset;
import java.io.*;

public class RUStoreClient {

	/* any necessary class members here */

	Socket clientSocket;
	private String h; // host ip
	private int p; // port
	private PrintWriter out ;
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
		out = new PrintWriter(clientSocket.getOutputStream(), true) ;
		//dOut = new DataOutputStream(clientSocket.getOutputStream()) ;
		in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream())) ;
		//dIn = new DataInputStream(clientSocket.getInputStream()) ;

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
		out.println("PUT DATA") ;

		System.out.println("wrote, waiting...");

		String response = in.readLine() ;
		System.out.println("response: " + response);

		if (response.equals("KEY?")) {

			out.println(key) ;

			response = in.readLine() ;

			if (response.equals("EXISTS")) {

				System.out.println("Key already exists") ;
				return 1 ;

			} else if (response.equals("OPEN")) {

				System.out.println("Key does not exist") ;
				//System.out.println("data length = " + data.length) ;
				//out.println(data.length) ;
				out.println(new String(data)) ;


			} else { // Invalid response

				return -1 ;

			}
		
		} else { // Invalid response

			return -1 ;

		}

		return 0 ;

	}

	/**
	 * Sends an arbitrary data object to the object store server. If an
	 * object with the same key already exists, the object should NOT
	 * be overwritten.
	 * 
	 * @param key       key to be used as the unique identifier for the object
	 * @param file_path path of file data to transfer
	 * 
	 * @return		0 upon success
	 *        		1 if key already exists
	 *        		Throw an exception otherwise
	 */
	public int put(String key, String file_path) {

		// Implement here
		return -1;

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
		out.println("GET DATA") ;

		System.out.println("wrote, waiting...");

		String response = in.readLine() ;
		System.out.println("response: " + response);

		if (response.equals("KEY?")) {

			out.println(key) ;

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
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server and places it in a file.
	 * 
	 * @param key	key associated with the object
	 * @param	file_path	output file path
	 * 
	 * @return		0 upon success
	 *        		1 if key doesn't exist
	 *        		Throw an exception otherwise
	 */
	public int get(String key, String file_path) {

		// Implement here
		return -1;

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

		out.println("DISCONNECT") ;

		clientSocket.close();

		System.out.println("Disconnected!");

	}

}
