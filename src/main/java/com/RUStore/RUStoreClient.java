package com.RUStore;

/* any necessary Java packages here */
import java.net.*;
import java.io.*;

public class RUStoreClient {

	/* any necessary class members here */

	Socket clientSocket;
	private String h; // host ip
	private int p; // port
	private PrintWriter out ;
	private DataOutputStream dOut ;
	private BufferedReader in ;


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
		dOut = new DataOutputStream(clientSocket.getOutputStream()) ;
		in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream())) ;

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
		out.println("PUT DATA") ;

		if (!in.readLine().equals("KEY?")) {

			return -1 ;

		}

		out.println(key) ;

		String response = in.readLine() ;

		if (response.equals("EXISTS")) {

			return 1 ;

		} else if (!response.equals("DATA?")) {

			return -1 ;

		}

		dOut.writeInt(data.length) ;
		dOut.write(data) ;

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
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server.
	 * 
	 * @param key key associated with the object
	 * 
	 * @return object data as a byte array, null if key doesn't exist. Throw an
	 *         exception if any other issues occur.
	 */
	public byte[] get(String key) {

		// Implement here
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

		clientSocket.close();

		System.out.println("Disconnected!");

	}

}
