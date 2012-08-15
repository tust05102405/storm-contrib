package storm.kafka;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.transactional.state.TransactionalState;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import kafka.message.Message;
import storm.kafka.PartitionManager.KafkaMessageId;

// TODO: need to add blacklisting
// TODO: need to make a best effort to not re-emit messages if don't have to
public class KafkaSpout extends BaseRichSpout {
    public static class ZooMeta implements Serializable {
        String id;
        long offset;

        public ZooMeta() {

        }

        public ZooMeta(String id, long offset) {
            this.id = id;
            this.offset = offset;
        }
    }

    public static class MessageAndRealOffset {
        public Message msg;
        public long offset;

        public MessageAndRealOffset(Message msg, long offset) {
            this.msg = msg;
            this.offset = offset;
        }
    }

    static enum EmitState {
        EMITTED_MORE_LEFT,
        EMITTED_END,
        NO_EMITTED
    }

    public static final Logger LOG = Logger.getLogger(KafkaSpout.class);

    

    String _uuid = UUID.randomUUID().toString();
    SpoutConfig _spoutConfig;
    SpoutOutputCollector _collector;
    TransactionalState _state;
    PartitionCoordinator _coordinator;
    DynamicPartitionConnections _connections;
    
    long _lastUpdateMs = 0;

    int _currPartitionIndex = 0;
    AtomicInteger ackCount = new AtomicInteger(0);
    AtomicInteger emitCount = new AtomicInteger(0);
    final long startTs = System.currentTimeMillis();

    public KafkaSpout(SpoutConfig spoutConf) {
        _spoutConfig = spoutConf;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        Map stateConf = new HashMap(conf);

        _connections = new DynamicPartitionConnections(_spoutConfig);
        List<String> zkServers = _spoutConfig.zkServers;
        if(zkServers==null) zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);

        Integer zkPort = _spoutConfig.zkPort;
        if(zkPort==null) zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();

        String zkRoot = _spoutConfig.zkRoot;

        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, zkRoot);

        Config componentConf = new Config();
        componentConf.registerSerialization(ZooMeta.class);

        // using TransactionalState like this is a hack
        _state = TransactionalState.newUserState(stateConf, _spoutConfig.id, componentConf);
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        if(_spoutConfig.hosts instanceof KafkaConfig.StaticHosts) {
            _coordinator = new StaticCoordinator(_connections, _spoutConfig, context.getThisTaskIndex(), totalTasks, _state, _uuid);
        } else {
            _coordinator = new ZkCoordinator(_connections, conf, _spoutConfig, context.getThisTaskIndex(), totalTasks, _state, _uuid);
        }

    }

    @Override
    public void nextTuple() {
        List<PartitionManager> managers = _coordinator.getMyManagedPartitions();
        for(int i=0; i<managers.size(); i++) {
            
            // in case the number of managers decreased
            _currPartitionIndex = _currPartitionIndex % managers.size();
            EmitState state = managers.get(_currPartitionIndex).next(_collector);
            emitCount.incrementAndGet();
            if (Math.random() < 0.01 && state != EmitState.NO_EMITTED && (System.currentTimeMillis() - startTs) > 1000) {
                LOG.info("emit speed about: " + (emitCount.get() / ((System.currentTimeMillis() - startTs) / 1000)) + " msg/s.");
            }
            if(state!=EmitState.EMITTED_MORE_LEFT) {
                _currPartitionIndex = (_currPartitionIndex + 1) % managers.size();
            }
            if(state!=EmitState.NO_EMITTED) {
                LOG.info("EmitState.NO_EMITTED");
                break;
            }
        }

        long now = System.currentTimeMillis();
        if((now - _lastUpdateMs) > _spoutConfig.stateUpdateIntervalMs) {
            commit();
        }
    }

    @Override
    public void ack(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        PartitionManager m = _coordinator.getManager(id.partition);
        if(m!=null) {
            m.ack(id.offset);
            ackCount.incrementAndGet();
            if (Math.random() < 0.01 && (System.currentTimeMillis() - startTs) > 1000) {
                LOG.info("ack speed about: " + (ackCount.get() / ((System.currentTimeMillis() - startTs) / 1000)) + " msg/s.");
            }
        }                
    }

    @Override
    public void fail(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        PartitionManager m = _coordinator.getManager(id.partition);
        if(m!=null) {
            m.fail(id.offset);
        } 
    }

    @Override
    public void deactivate() {
        commit();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_spoutConfig.scheme.getOutputFields());
    }

    private void commit() {
        _lastUpdateMs = System.currentTimeMillis();
        for(PartitionManager manager: _coordinator.getMyManagedPartitions()) {
            manager.commit();
        }
    }

    public static void main(String[] args) {
//        TopologyBuilder builder = new TopologyBuilder();
//        List<String> hosts = new ArrayList<String>();
//        hosts.add("localhost");
//        SpoutConfig spoutConf = SpoutConfig.fromHostStrings(hosts, 8, "clicks", "/kafkastorm", "id");
//        spoutConf.scheme = new StringScheme();
//        spoutConf.forceStartOffsetTime(-2);
//
// //       spoutConf.zkServers = new ArrayList<String>() {{
// //          add("localhost");
// //       }};
// //       spoutConf.zkPort = 2181;
//
//        builder.setSpout("spout", new KafkaSpout(spoutConf), 3);
//
//        Config conf = new Config();
//        //conf.setDebug(true);
//
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("kafka-test", conf, builder.createTopology());
//
//        Utils.sleep(600000);
    }
}
