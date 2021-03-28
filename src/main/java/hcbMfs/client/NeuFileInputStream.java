package hcbMfs.client;

import javafx.util.Pair;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NeuFileInputStream extends FSInputStream {

    private byte[] byteBuffer ;

    private int pointer;

    private CuratorFramework client;

    private KafkaConsumer<String, byte[]> consumer;

    public NeuFileInputStream(CuratorFramework zkclient, String path, KafkaConsumer<String, byte[]> consum) {
        this.client = zkclient;
        this.consumer = consum;

        // get offset from zookeeper
        byte[] output = new byte[0];
        try {
            output = client.getData().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
        long offset = pathInfo.fileInfo.offset;

        // get message from kafka
        Pair<String, Integer> tp = getTopicPatition(pathInfo.name);
        TopicPartition topicPartition = new TopicPartition(tp.getKey(),tp.getValue() % MfsFileSystem.partNum);
        List topicPartitionList = new ArrayList<TopicPartition>();
        topicPartitionList.add(topicPartition);
        consumer.assign(topicPartitionList);
        consumer.seek(topicPartition,offset);
        ConsumerRecords<String, byte[]> records;
        while (true) {
            records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
            if(!records.isEmpty()){
                break;
            }
        }
        Iterator<ConsumerRecord<String, byte[]>> iterator =records.iterator();
        ConsumerRecord<String, byte[]> record = iterator.next();
        byteBuffer = record.value();
        // solved org.apache.kafka.common.KafkaException: Failed to construct kafka consumer
        consum.close();
    }

    @Override
    public int read() throws IOException {
        if(pointer < byteBuffer.length){
            int res = (int)byteBuffer[pointer];
            pointer++;
            return res&(0xff);
        }
        return -1;
    }

    private Pair<String, Integer> getTopicPatition(String filePath) {
        String[] subPaths = filePath.split("/");
        Pair<String, Integer> pair = new Pair<>(subPaths[1], Integer.parseInt(subPaths[2]));
        return pair;
    }


    @Override
    public void seek(long pos) throws IOException {

    }

    @Override
    public long getPos() throws IOException {
        return 0;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }
}
