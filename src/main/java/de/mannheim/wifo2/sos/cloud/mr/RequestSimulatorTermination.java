package de.mannheim.wifo2.sos.cloud.mr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

/**
 * The request simulator randomly sends requests to the server
 * @author Max
 *
 */
public class RequestSimulatorTermination extends Thread  {
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private int step = 0;

    public RequestSimulatorTermination(InetAddress address, int port)  {
        try {
            this.socket = new Socket(address, port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
		
		/*
		 * each 15 seconds your algorithm is called
		 */
        Thread t = new Thread()  {
            public void run()  {
                while(step < 6000)  {
                    try {
                        Thread.sleep(15000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    out.println("5");	//first start processes

                    if(Configuration.LOG_REQUESTS)  System.out.println("ALGORITHM called");

                    String result = null;
                    try {
                        result = in.readLine();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    if(Configuration.LOG_REQUESTS)  System.out.println("ALGORITHM Result: " + result);

                    out.println("4");	//then start termination algorithm
                    //TODO the credit method may be implemented within the processes.
                    //TODO As the 4 counter method must go through all processes using the neighbors
                    //the echo algorithm might be useful for preventing revisits.

                    if(Configuration.LOG_REQUESTS)  System.out.println("ALGORITHM called");

                    result = null;
                    try {
                        result = in.readLine();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    if(Configuration.LOG_REQUESTS)  System.out.println("ALGORITHM Result: " + result);
                }
            }
        };
        t.start();
    }

    @Override
    public void run()  {
        int random = (int)(Math.random()*10+11);	//random number (11-20) of VMs that should be added
        while(step < random)  {
            addRequest();	//first add VMs
        }
        while(step < 6000)  {
            request();		//store and workload methods
        }
        System.exit(0);
    }

    private void addRequest()  {
        String command = null;
        command = "3";		//add VM
        step++;

        out.println(command);
        if(Configuration.LOG_REQUESTS)  System.out.println("Command: " + command);

        String result = null;
        try {
            int i = 0;
            while(i < 5)  {
                result = in.readLine();
                if(Configuration.LOG_REQUESTS)  System.out.println(result);
                i++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * requests are created here.
     * Probabilities:
     * store(key, value) 50%
     * workload() 50%
     */
    private void request()  {
        String command = null;
        int random = (int)(Math.random()*100);
        if((random < 50))  {
            command = "2";	//workload
        }
        else  {
            command = "1";	//store
            command += ";";

            //compute random key
            int loop = (int)(Math.random()*10 + 3);
            for(int i=0 ; i<loop ; i++)  {
                char letter = (char)(Math.random()*26 + 97);
                command += letter;
            }

            command += ";";

            //compute random value
            int value = (int)(Math.random()*10000);
            command += value;
        }

        step++;

        out.println(command);
        if(Configuration.LOG_REQUESTS)  System.out.println("Command: " + command);

        String result = null;
        try {
            result = in.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(Configuration.LOG_REQUESTS)  System.out.println(result);

        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException  {
        Thread rs = new RequestSimulatorTermination(InetAddress.getLoopbackAddress(), 9000);
        rs.start();
    }
}