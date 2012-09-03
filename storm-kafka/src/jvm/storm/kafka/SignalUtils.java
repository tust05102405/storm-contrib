package storm.kafka;

import java.io.Serializable;

import backtype.storm.contrib.signals.client.SignalClient;

public class SignalUtils implements Serializable {
    private String zkHost;
    private String signalName;
    public SignalUtils(String zkHost, String signalName) {
        this.zkHost = zkHost;
        this.signalName = signalName;
    }
    
    public void send(String signal) throws Exception {
        SignalClient signalClient = new SignalClient(zkHost, signalName);
        signalClient.start();
        signalClient.send(signal.getBytes());
        signalClient.close();
    }
}
