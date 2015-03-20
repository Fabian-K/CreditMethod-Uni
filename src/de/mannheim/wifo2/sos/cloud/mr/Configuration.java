package de.mannheim.wifo2.sos.cloud.mr;

public class Configuration {
    public final static Boolean LOG_SERVER = false;        //set false if you don't wish to see server messages
    public final static Boolean LOG_VMINTERFACE = false;    //set false if you don't wish to see vmInterface messages
    public final static Boolean LOG_VMS = false;            //set false if you don't wish to see VM messages
    public final static Boolean LOG_REQUESTS = false;    //set false if you don't wish to see request simulator messages
    public final static Boolean LOG_PROCESSES = true;    //set false if you don't wish to see process messages

    public final static Integer NR_NEIGHBORS = 2;        //number of neighbors that new VMs get initially
}
