package util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DirectReader implements Closeable {

  private static final Logger LOGGER =
          LoggerFactory.getLogger(DirectReader.class.getName());

  private RandomAccessFile in;
  private FileChannel ch;
  private ByteBuffer buf;

  String path;

  public DirectReader(String path) throws IOException{
    this(path, 65536);
  }

  public DirectReader(String path, int bufferSize) throws IOException {

    in = new RandomAccessFile(path, "r");
    ch = in.getChannel();
    try{
      buf = ByteBuffer.allocateDirect(bufferSize);
    } catch (OutOfMemoryError e){
      LOGGER.warn("Failed to allocate a direct buffer. Use a general byte buffer instead.");

      buf = ByteBuffer.allocate(bufferSize);
    }
    buf.flip();

    this.path = path;
  }

  public String getPath(){
    return path;
  }

  private void loadNext() throws IOException{

    buf.clear();
    ch.read(buf);
    buf.flip();

  }

  /**
   * read 32-bit data as an integer type value.
   * @return 32-bit data in an integer value
   * @throws IOException
   */
  public int readInt() throws IOException{

    int u;

    try {
      u = buf.getInt();
    } catch(BufferUnderflowException e){

      loadNext();

      if(buf.hasRemaining()){
        u = buf.getInt();
      }
      else throw new EOFException();
    }

    return u;

  }

  /**
   * read 64-bit data as a long type value.
   * @return 64-bit data in a long type value.
   * @throws IOException
   */
  public long readLong() throws IOException{

    long u;

    try {
      u = buf.getLong();
    } catch(BufferUnderflowException e){

      loadNext();

      if(buf.hasRemaining()){
        u = buf.getLong();
      }

      else throw new EOFException();
    }

    return u;

  }

  /**
   * check there is more data to read.
   * @return true if data remain, false otherwise
   * @throws IOException
   */
  public boolean hasNext() throws IOException {
    return buf.hasRemaining() || (ch.size() > ch.position());
  }

  /**
   * close this reader.
   * @throws IOException
   */
  public void close() throws IOException {
    in.close();
    ch.close();
    buf = null;
  }


}
