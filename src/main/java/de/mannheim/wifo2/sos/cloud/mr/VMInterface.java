package de.mannheim.wifo2.sos.cloud.mr;

import java.util.*;

/**
 * This is the interface between a server and the virtual machines
 * on it. Can be seen as a (bad) load balancer.
 *
 * @author Max
 */
public class VMInterface {
    private ArrayList<VirtualMachine> vms;    //list of VMs
    private Integer idCount = 1;            //ID count for new VMs

    //TODO You may extend class variables for your algorithm
    private int idProcess = 1;
    private Server server;

    /**
     * contains the set of missing credits for each process id
     */
    private Map<Integer, Set<Integer>> missingCreditMap;

    /**
     * Constructor
     * Initializes the list of VMs
     *
     * @param server
     */
    public VMInterface(Server server) {
        vms = new ArrayList<VirtualMachine>();
        missingCreditMap = new HashMap<Integer, Set<Integer>>();
        this.server = server;
    }

    /**
     * adds a new VM to the list and randomly assigns neighbors
     *
     * @return the new VM
     */
    public VirtualMachine addVM() {
        if (Configuration.LOG_VMINTERFACE) System.out.println("VMInterface: addVM()");
        VirtualMachine vm = null;

        if (vms.size() >= 1) {
            //Randomly assigned neighbor VMs
            int neighbors = (vms.size() >= Configuration.NR_NEIGHBORS) ?
                    Configuration.NR_NEIGHBORS : vms.size();
            Integer[] chosenNeighbors = new Integer[neighbors];
            int count = 0;

            ArrayList<VirtualMachine> neighborVMs = new ArrayList<VirtualMachine>();
            while (count < neighbors) {
                int random = (int) (Math.random() * vms.size());
                boolean in = false;
                for (Integer i : chosenNeighbors) {
                    if ((null != i) && (random == i)) {
                        in = true;
                    }
                }

                if (!in) {
                    neighborVMs.add(vms.get(random));
                    chosenNeighbors[count] = random;
                    count++;
                }
            }

            //Create VM
            vm = new VirtualMachine("VM" + idCount, neighborVMs, this);
            idCount++;

            //Update neighbors
            for (int i : chosenNeighbors) {
                vms.get(i).addNeighbor(vm);
            }

            //Add VM to list
            vms.add(vm);
        } else {
            //If list is empty add new VM with empty neighbor list
            vm = new VirtualMachine("VM" + idCount,
                    new ArrayList<VirtualMachine>(), this);
            vms.add(vm);
            idCount++;
        }

        return vm;
    }

    /**
     * stores a (key, value)-pair at the VM with the minimum number
     * of database entries. If no VM is available, a new VM is created
     *
     * @param key
     * @param value
     * @return value
     */
    public Integer store(String key, Integer value) {
        if (Configuration.LOG_VMINTERFACE) System.out.println("VMInterface: store(" + key + ", " + value + ")");

        if (vms.size() == 0) {
            if (Configuration.LOG_VMINTERFACE) System.out.println("VMInterface: No VM available");
            addVM();
        }

        int min = 9999999;
        VirtualMachine vm = null;

        for (VirtualMachine v : vms) {
            if (v.getStorage().size() < min) {
                min = v.getStorage().size();
                vm = v;
            }
        }

        Integer result = -1;
        if (vm != null) {
            result = vm.addEntry(key, value);
        }

        return result;
    }

    /**
     * "Calculates" the workload/storage usage of a random VM
     *
     * @return number of database entries
     */
    public Integer workload() {
        if (Configuration.LOG_VMINTERFACE) System.out.println("VMInterface: workload()");

        if (vms.size() == 0) {
            return -1;
        }

        Integer random = (int) (Math.random() * vms.size());
        final VirtualMachine vm = vms.get(random);
        Integer result = vm.workload();

        return result;
    }

    /**
     * starts a process on a random VM
     * the process gets a hopcounter of 5-9
     */
    public int startProcesses() {
        int processId = idProcess;

        if (Configuration.LOG_PROCESSES || Configuration.LOG_VMINTERFACE) {
            System.out.println("VMInterface: ---------------------------------------------");
            System.out.println("VMInterface: startCalculations() with id " + processId);
        }

        if (vms.size() == 0) {
            return -1;
        }

        int random = (int) (Math.random() * vms.size());
        final VirtualMachine vm = vms.get(random);

        random = (int) (Math.random() * 5 + 5);

        // initialize
        Set<Integer> missingCredits = new HashSet<Integer>();
        missingCredits.add(1);
        missingCreditMap.put(processId, missingCredits);

        vm.startProcess(random, processId, 1);

        idProcess++;

        return processId;
    }

    /**
     * starts the algorithm you should implement
     * on a random VM
     *
     * @return state algos should return table (HashMap) with all
     * database entries, 4 counter should notify that the processes
     * have terminated
     */
    public Object yourAlgorithm() {
        if (Configuration.LOG_VMINTERFACE) System.out.println("VMInterface: yourAlgorithm()");

        if (vms.size() == 0) {
            return null;
        }

        Integer random = (int) (Math.random() * vms.size());
        final VirtualMachine vm = vms.get(random);
        Object result = vm.yourAlgorithm();

        return result;
    }

    // a process, initially triggered by rootProcessId finished its calculations
    // transfer credit back
    public synchronized void notifyProcessFinished(int rootProcessId, int credit) {
        System.out.println("VMInterface: Receiving " + credit + " for Process " + rootProcessId);
        Set<Integer> missingCredits = missingCreditMap.get(rootProcessId);
        while (!missingCredits.contains(credit)) {
            missingCredits.add(credit);
            credit--;
        }

        missingCredits.remove(credit);

        if (missingCredits.isEmpty()) {
            System.out.println("VMInterface: Process " + rootProcessId + " is quiescent");
            System.out.println("VMInterface: ---------------------------------------------");
            missingCreditMap.remove(rootProcessId);
            server.processDidFinish(rootProcessId);
        }
    }
}
