package hcbMfs.client;

import javafx.util.Pair;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class NeuFileOutputStream extends OutputStream {

    private int BYTE_BUFFER_SIZE;
    private byte[] byteBuffer;
    private int pointer;

    private PathInfo pathInfo;

    private CuratorFramework client;

    private Producer<String, byte[]> producer;


    public NeuFileOutputStream(CuratorFramework zkclient,String path,Producer<String, byte[]> produc) {
        // path:  /topic/partitionNo/fileName
        this.BYTE_BUFFER_SIZE = MfsFileSystem.BUFFERSIZE;
        byteBuffer = new byte[BYTE_BUFFER_SIZE];
        this.client = zkclient;
        pathInfo = new PathInfo();
        pathInfo.name = path;
        pathInfo.isDirectory = false;
        this.producer = produc;
    }

    @Override
    public void write(int b) throws IOException {
        byteBuffer[pointer] = (byte) b;
        pointer++;
    }


    @Override
    public void close() throws IOException {
        // 写入kafka
        Pair<String, Integer> topicPartition = getTopicPatition(pathInfo.name);
        ProducerRecord record =
                new ProducerRecord(topicPartition.getKey(), topicPartition.getValue()%3,
                        pathInfo.name, Arrays.copyOf(byteBuffer,pointer));
        Future<RecordMetadata> future = producer.send(record);
        // 下边这句代码必须有,会刷新缓存到主题.
        producer.flush();
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        producer.close();

        // 写元信息
        pathInfo.fileInfo.hasRenamed = false;
        pathInfo.fileInfo.offset = recordMetadata.offset();
        pathInfo.fileInfo.contentLength = pointer;
        pathInfo.lastModified = System.currentTimeMillis();
        pathInfo.fileInfo.contentHash = "";
        byte[] input = SerializationUtils.serialize(pathInfo);
        try {
            if(client.checkExists().forPath(pathInfo.name) == null){
                client.create()
                        .creatingParentContainersIfNeeded()
                        .forPath(pathInfo.name, input);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }




    private Pair<String, Integer> getTopicPatition(String filePath) {
        String[] subPaths = filePath.split("/");
        Pair<String, Integer> pair = new Pair<>(subPaths[1], Integer.parseInt(subPaths[2]));
        return pair;
    }

}