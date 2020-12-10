# Usage

- path约定
  - /topic/partitionNo/fileName
  - partitionNo必须是数字
- 文件大小小于10兆

FileSystem支持api

增：

```java
public FSDataOutputStream create(Path f) throws IOException 
```

删：删除目录只支持topic级别，不支持partition级别删除

```java
/** Delete a file.
 *
 * @param f the path to delete.
 * @param recursive if path is a directory and set to 
 * true, the directory is deleted else throws an exception. In
 * case of a file the recursive can be set to either true or false. 
 * @return  true if delete is successful else false. 
 * @throws IOException
 */
public abstract boolean delete(Path f, boolean recursive) throws IOException;
```

改：不支持文件内容修改，仅支持重命名，且src和dst只能是相同主题和相同分区，path只能是文件

```java
/**
 * Renames Path src to Path dst.  Can take place on local fs
 * or remote DFS.
 * @param src path to be renamed
 * @param dst new path after rename
 * @throws IOException on failure
 * @return true if rename is successful
 */
public abstract boolean rename(Path src, Path dst) throws IOException;
```

读取文件：

```java
/**
 * Opens an FSDataInputStream at the indicated Path.
 * @param f the file name to open
 * @param bufferSize the size of the buffer to be used.
 */
public abstract FSDataInputStream open(Path f, int bufferSize)
  throws IOException;
```



## 示例

