package hcbMfs.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyUtils {

    public static String getLogPath(){
        // 动态加载操作符个数
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream inStream = cl.getResourceAsStream("hcbConfig.properties");
        Properties myProp = new Properties();
        try {
            myProp.load(inStream);
        } catch (IOException e) {
            MfsFileSystem.LOG.error(e.toString());
        }
        return myProp.getProperty("logPath");
    }


    public static String getZkNameSpace(){
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream inStream = cl.getResourceAsStream("hcbConfig.properties");
        Properties myProp = new Properties();
        try {
            myProp.load(inStream);
        } catch (IOException e) {
            MfsFileSystem.LOG.error(e.toString());
        }
        return myProp.getProperty("zkNameSpace");
    }
    public static int getBufferSize() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream inStream = cl.getResourceAsStream("hcbConfig.properties");
        Properties myProp = new Properties();
        try {
            myProp.load(inStream);
        } catch (IOException e) {
            MfsFileSystem.LOG.error(e.toString());
        }
        return Integer.parseInt(myProp.getProperty("bufferSize"));

    }
    public static int getPartNum() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream inStream = cl.getResourceAsStream("hcbConfig.properties");
        Properties myProp = new Properties();
        try {
            myProp.load(inStream);
        } catch (IOException e) {
            MfsFileSystem.LOG.error(e.toString());
        }
        return Integer.parseInt(myProp.getProperty("partNum"));

    }


}
