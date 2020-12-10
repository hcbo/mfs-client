
package hcbMfs.client;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaFuture;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class NeuUnderFileSystem {

    public CuratorFramework client;

    AdminClient adminClient;

    KafkaProducer<String, byte[]> producer;

    String rootPath; // for example : china

    Properties properties = new Properties();

    /**
     * Constructs a new {@link NeuUnderFileSystem}.
     *
     * @param conf UFS configuration
     */
    public NeuUnderFileSystem(URI uri, Configuration conf) {

        MfsFileSystem.LOG.info("NeuUnderFileSystem created");
        this.rootPath = PropertyUtils.getZkNameSpace();
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        String zkServers = MfsFileSystem.ZK_SERVERS;
        client = CuratorFrameworkFactory.builder()
                .connectString(zkServers)
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(6000)
                .connectionTimeoutMs(3000)
                .namespace(rootPath)
                .build();
        client.start();

        //property of Kafka
        String kafkaServers = MfsFileSystem.KAFKA_SERVERS;
        properties.put("bootstrap.servers", kafkaServers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("enable.auto.commit", true);
        properties.put("group.id", "test10");
        properties.put("auto.offset.reset", "earliest");
        properties.put("acks", "-1");
        properties.put("retries", 3);
        properties.put("buffer.memory", 33554432);
        properties.put("max.partition.fetch.bytes", MfsFileSystem.BUFFERSIZE);
        properties.put("fetch.max.bytes", MfsFileSystem.BUFFERSIZE);

        adminClient = AdminClient.create(properties);
        try {
            producer = new KafkaProducer<>(properties);
        } catch (Exception e) {
            MfsFileSystem.LOG.error(e.getMessage());
        }

        MfsFileSystem.LOG.info("NeuUnderFileSystem construct completed");
    }


    public OutputStream create(String path) throws IOException {
        KafkaProducer<String, byte[]> produc = null;
        try {
//          Thread.currentThread().setContextClassLoader(null);
            produc = new KafkaProducer<String, byte[]>(properties);
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            MfsFileSystem.LOG.error("exception stack: " + sw.toString());
        }

        return new NeuFileOutputStream(client, stripPath(path), produc);
    }


    public boolean deleteFile(String path) {
        path = stripPath(path);
        // remove first /
        String topicName = path.substring(1);
        if (!topicName.contains("/")) {
            // topic
            Set<String> topics = new HashSet<>();
            topics.add(topicName);
            DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topics);
            Map<String, KafkaFuture<Void>> result = deleteTopicsResult.values();
            KafkaFuture<Void> kafkaFuture = result.get(topicName);
            try {
                kafkaFuture.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        try {
            client.delete()
                    .guaranteed()      //删除失败，则客户端持续删除，直到节点删除为止
                    .deletingChildrenIfNeeded()   //删除相关子节点
                    .withVersion(-1)    //无视版本，直接删除
                    .forPath(path);
        } catch (Exception e) {
            MfsFileSystem.LOG.error(e.toString());
        }
        return true;
    }


    public boolean exists(String path) throws IOException {
        String underPath = path;
        if (path.contains(MfsFileSystem.FS_MFS_DEFAULT_PORT + "")) {
            underPath = stripPath(path);
        }
        try {
            return null != client.checkExists().forPath(underPath);
        } catch (Exception e) {
            MfsFileSystem.LOG.error(e.getMessage());
            e.printStackTrace();
        }
        return false;
    }








    public FileStatus[] listStatus(String path) throws IOException {
        String underPath = stripDirPath(path);
        // 根据zk 获取子节点 getchildlen
        List<String> children = null;
        try {
            children = client.getChildren().forPath(underPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        FileStatus[] rtn = new FileStatus[children.size()];
        if (children != null && children.size() != 0) {

            int i = 0;
            for (String child : children) {
                String childPath = underPath + "/" + child;
                FileStatus retStatus;
                // 取元信息出来
                byte[] output = new byte[0];
                try {
                    output = client.getData().forPath(childPath);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
                FsPermission permission = FsPermission.createImmutable((short) 777);
                retStatus = new FileStatus(pathInfo.fileInfo.contentLength, pathInfo.isDirectory,
                        1, 512, pathInfo.lastModified, 0, permission, null,
                        null, null, new Path(childPath));

                rtn[i++] = retStatus;
            }
            return rtn;
        } else {
            return rtn;
        }

    }

    /**
     * the format of path below
     * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1
     * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/offsets
     * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/sources
     * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/sources/0
     * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state
     * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state/0
     * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state/0/0
     * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/commits
     */

    public boolean mkdirs(String path) throws IOException {
        // 传入的一定是目录的路径
        String underPath = stripDirPath(path);
        if (exists(underPath)) {
            return false;
        } else {
            // save to zookeeper
            PathInfo pathInfo = new PathInfo();
            pathInfo.name = underPath;
            pathInfo.isDirectory = true;
            byte[] input = SerializationUtils.serialize(pathInfo);
            try {
                client.create()
                        .creatingParentContainersIfNeeded()
                        .forPath(underPath, input);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return true;
        }
    }

    private String stripDirPath(String path) {
        String divSign = MfsFileSystem.FS_MFS_DEFAULT_PORT + "";
        int begin = path.indexOf(divSign) + divSign.length();
        return path.substring(begin);
    }




    public InputStream open(String path) throws IOException {

        KafkaConsumer<String, byte[]> consum = null;
        try {
//          Thread.currentThread().setContextClassLoader(null);
            consum = new KafkaConsumer<String, byte[]>(properties);
            return new NeuFileInputStream(client, stripPath(path), consum);
        } catch (Exception e) {
            MfsFileSystem.LOG.error("exception " + e.toString());
            return null;
        }
    }


    public boolean renameFile(String src, String dst) throws IOException {
        src = stripPath(src);
        dst = stripPath(dst);

        byte[] output = new byte[0];
        try {
            output = client.getData().forPath(src);
        } catch (Exception e) {
            e.printStackTrace();
        }
        PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
        pathInfo.fileInfo.hasRenamed = true;
        pathInfo.name = dst;
        // remove old
        try {
            client.delete()
                    .guaranteed()      //删除失败，则客户端持续删除，直到节点删除为止
                    .deletingChildrenIfNeeded()   //删除相关子节点
                    .withVersion(-1)    //无视版本，直接删除
                    .forPath(src);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // insert new
        byte[] input = SerializationUtils.serialize(pathInfo);
        try {
            client.create()
                    .creatingParentContainersIfNeeded()
                    .forPath(dst, input);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }



    private String stripPath(String path) {
//        // mfs://localhost:8888/topic/partitionNo/fileName
//        String divSign = MfsFileSystem.FS_MFS_DEFAULT_PORT + "";
//        int begin = path.indexOf(divSign) + divSign.length();
//        // /topic/partitionNo/fileName
//        return path.substring(begin);
        return path;
    }


    public FileStatus getFileStatus(Path path) {
        String curPath = stripDirPath(path.toString());
        try {
            if (null == client.checkExists().forPath(curPath)) {
                return null;
            } else {
                byte[] output = new byte[0];
                output = client.getData().forPath(curPath);
                PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
                if (pathInfo.isDirectory) {
                    return new FileStatus();
                } else {
                    if (!pathInfo.fileInfo.hasRenamed) {
                        return null;
                    } else {
                        return new FileStatus();
                    }
                }
            }
        } catch (Exception e) {
            MfsFileSystem.LOG.error("Neu getFileStatus " + "path:" + path + " " + e.toString());
        }
        return null;
    }


    public FSDataOutputStream nop() {
        return null;
    }
}
