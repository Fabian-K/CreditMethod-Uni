package de.mannheim.wifo2.sos.cloud.mr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

public class VirtualMachine {
    private String id;                                //VM ID
    private Boolean isActive;                        //false if no process is running
    private HashMap<String, Integer> storage;        //database
    private ArrayList<VirtualMachine> neighbors;    //list of neighbors
    private VMInterface vmInterface;                //vmInterface (required for credit method to notify VMInterface)

    //TODO You may extend class variables for your algorithm

    /**
     * Constructor
     * Initializes class variables
     *
     * @param id          VM ID
     * @param neighbors   list of neighbors
     * @param vmInterface vmInterface
     */
    public VirtualMachine(String id, ArrayList<VirtualMachine> neighbors,
                          VMInterface vmInterface) {
        this.id = id;
        this.neighbors = neighbors;
        this.isActive = false;
        this.storage = new HashMap<String, Integer>();
        this.vmInterface = vmInterface;
    }

    /**
     * Gets VM ID
     *
     * @return id
     */
    public String getVMId() {
        return id;
    }

    /**
     * Gets if active
     *
     * @return isActive
     */
    public Boolean isActive() {
        return isActive;
    }

    /**
     * Gets the database
     *
     * @return storage
     */
    public HashMap<String, Integer> getStorage() {
        return storage;
    }

    /**
     * adds an entry to the database and initiates
     * replication at neighbors
     *
     * @param key
     * @param value
     * @return value
     */
    public Integer addEntry(String key, Integer value) {
        Integer result = storage.put(key, value);
        if (Configuration.LOG_VMS) System.out.println(id + ": addEntry(" + key + ", " + value + ")");

        for (VirtualMachine v : neighbors) {
            v.addReplica(key, value);
        }

        return result;
    }

    /**
     * replicates a (key, value)-pair
     *
     * @param key
     * @param value
     * @return value
     */
    private Integer addReplica(String key, Integer value) {
        Integer result = storage.put(key, value);
        if (Configuration.LOG_VMS) System.out.println(id + ": addReplica(" + key + ", " + value + ")");
        return result;
    }

    /**
     * Adds a neighbor to the neighbor list
     *
     * @param vm neighbor
     */
    public void addNeighbor(VirtualMachine vm) {
        neighbors.add(vm);
        if (Configuration.LOG_VMS) System.out.println(id + ": addNeighbor(" + vm.getVMId() + ")");
    }

    /**
     * "Calculates" the workload/storage usage of the database
     *
     * @return number of database entries
     */
    public Integer workload() {
        if (Configuration.LOG_VMS) System.out.println(id + ": workload()");
        return storage.size();
    }

    /**
     * starts a process on this VM that starts further processes on
     * 1-2 neighbors that starts further... until hopcounter is 0
     * while the VM is processing something it is active, i.e.,
     * isActive = true
     *
     * @param hops
     */
    public void startProcess(int hops) {
        if (Configuration.LOG_VMS || Configuration.LOG_PROCESSES)
            System.out.println(id + ": startCalculations(" + hops + ")");

        final int hop = hops;
        Thread t = new Thread() {
            public synchronized void run() {
                isActive = true;
                int random1 = (int) (Math.random() * 2 + 1);        //number of neighbors which are
                //needed to finish the calculations
                for (int i = 0; i < random1; i++) {
                    int random2 = (int) (Math.random() * 200);
                    try {
                        Thread.sleep(random2);                //Simulating processing time
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    if (hop - 1 > 0) {
                        int neighbor = (int) (Math.random() * neighbors.size());    //randomly assign neighbor for calculations
                        neighbors.get(neighbor).startProcess(hop - 1);
                    }
                }

                isActive = false;
            }
        };

        t.start();
    }

    /**
     * This is your algorithm
     * Parameters may be added
     * Implement as Thread
     * Snapshot, PIF, 4 counter should be implemented here
     * credit method might be directly implemented in the
     * startProcess() method (credit method should use
     * the vmInterface as environment/p*)
     */
    public Object yourAlgorithm() {
        //TODO

        return "";
    }

    @Override
    public String toString() {
        String temp = "";
        temp += "ID : " + id + "\n";
        temp += "Active : " + isActive + "\n";
        temp += "Storage : \n";
        for (Entry<String, Integer> e : storage.entrySet()) {
            temp += e.getKey() + " : " + e.getValue() + "\n";
        }
        temp += "Neighbors : \n";
        for (VirtualMachine v : neighbors) {
            temp += v.getVMId() + ", ";
        }
        temp += "\n";
        return temp;
    }
}
