package de.mannheim.wifo2.sos.cloud.mr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * A server holds virtual machines and processes requests from
 * the request simulator. VMs are called via the VMInterface.
 *
 * @author Max
 */
public class Server extends Thread {

    private ServerSocket socket;        //Socket for accepting connections
    private Socket client;                //Client connection
    private BufferedReader in;            //Input stream from socket
    private PrintWriter out;            //Output stream from socket
    private VMInterface vmInterface;    //Interface for VMs

    /**
     * Constructor
     * Directly initializes the socket which then listens for
     * connections on the specified port
     *
     * @param id   server ID
     * @param port port for listening for connections
     */
    public Server(String id, int port) {
        this.vmInterface = new VMInterface();
        try {
            this.socket = new ServerSocket(port);
            if (Configuration.LOG_SERVER) System.out.println("Server: Waiting for connection");
            this.client = socket.accept();
            if (Configuration.LOG_SERVER)
                System.out.println("Server: Connection accepted with " + client.getInetAddress().toString());

            in = new BufferedReader(new InputStreamReader(client.getInputStream()));
            out = new PrintWriter(client.getOutputStream(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Thread server = new Server("Server1", 9000);
        server.start();
    }

    /**
     * forever listens for requests
     */
    @Override
    public void run() {
        while (true) {
            processRequest();
        }
    }

    /**
     * listens for requests
     */
    private void processRequest() {
        String received = null;
        try {
            received = in.readLine();    //reads requests
        } catch (IOException e) {
            e.printStackTrace();
        }
        String[] split = received.split(";");

        Integer split0 = Integer.parseInt(split[0]);

		/*
         * 1 -> store (key, value)-pair (1;key;value)
		 * 2 -> workload()
		 * 3 -> add virtual machine
		 * 4 -> the algorithm you should implement
		 * 5 -> start processes (termination algorithms should
		 * 		determine when these processes have terminated)
		 */
        Object result = null;
        switch (split0) {
            case 1:
                String key = split[1];
                Integer value = Integer.parseInt(split[2]);
                if (Configuration.LOG_SERVER) System.out.println("Server: store(" + key + ", " + value + ")");
                result = vmInterface.store(key, value);
                break;
            case 2:
                if (Configuration.LOG_SERVER) System.out.println("Server: workload()");
                result = vmInterface.workload();
                break;
            case 3:
                if (Configuration.LOG_SERVER) System.out.println("Server: addVM()");
                result = vmInterface.addVM();
                break;
            case 4:
                if (Configuration.LOG_SERVER) System.out.println("Server: YOUR COMMAND");
                result = vmInterface.yourAlgorithm();
                break;
            case 5:
                if (Configuration.LOG_SERVER || Configuration.LOG_PROCESSES)
                    System.out.println("Server: startCalculations()");
                vmInterface.startProcesses();
                result = "Calculations started";
                break;
            default:
                break;
        }
        out.println("Result: " + result);    //Request result is sent back to simulator
    }
}
