package util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DirectWriter implements Closeable {

  private static final Logger LOGGER =
          LoggerFactory.getLogger(DirectWriter.class.getName());

  RandomAccessFile in;
  FileChannel ch;
  ByteBuffer buf;

  String path;

  public DirectWriter(String path) throws IOException {
    this(path, 65536);
  }

  public DirectWriter(String path, int bufferSize) throws IOException {

    Files.createDirectories(Paths.get(path).getParent());
    Files.deleteIfExists(Paths.get(path));
    in = new RandomAccessFile(path, "rw");
    ch = in.getChannel();
    try{
      buf = ByteBuffer.allocateDirect(bufferSize);
    } catch (OutOfMemoryError e){
      LOGGER.warn("Failed to allocate a direct buffer. Use a general byte buffer instead.");
      buf = ByteBuffer.allocate(bufferSize);
    }

    buf.clear();

    this.path = path;
  }

  /**
   * write a 32-bit value.
   * @param u 32-bit value
   * @throws IOException
   */
  public void write(int u) throws IOException{

    try{
      buf.putInt(u);
    }catch(BufferOverflowException e){
      flush();
      buf.putInt(u);
    }

  }

  /**
   * write a 64-bit value.
   * @param u 64-bit value
   * @throws IOException
   */
  public void write(long u) throws IOException {

    try{
      buf.putLong(u);
    }catch(BufferOverflowException e){
      flush();
      buf.putLong(u);
    }

  }

  /**
   * close this writer.
   * @throws IOException
   */
  public void close() throws IOException {

    flush();

    ch.close();
    in.close();
    buf = null;
  }

  /**
   * flush the buffer.
   * @throws IOException
   */
  private void flush() throws IOException{
    buf.flip();
    ch.write(buf);
    buf.clear();
  }

}
