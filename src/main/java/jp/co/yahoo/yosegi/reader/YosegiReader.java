/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.co.yahoo.yosegi.reader;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.block.BlockReaderNameShortCut;
import jp.co.yahoo.yosegi.block.IBlockReader;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;
import jp.co.yahoo.yosegi.stats.SummaryStats;
import jp.co.yahoo.yosegi.util.FindClass;
import jp.co.yahoo.yosegi.util.io.InputStreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class YosegiReader implements AutoCloseable {

  private static final byte[] MAGIC = new byte[]{'$','C','L','M'};

  private final Map<String,IBlockReader> blockReaderMap = new HashMap<String,IBlockReader>();
  private final List<ReadBlockOffset> readTargetList = new ArrayList<ReadBlockOffset>();
  private IBlockReader currentBlockReader;
  private IExpressionNode blockSkipIndex;

  private InputStream in;
  private int blockSize;
  private long inReadOffset;

  private class FileHeaderMeta {
    public final int blockSize;
    public final int headerSize;
    public final String className;

    public FileHeaderMeta( final int blockSize , final String className , final int headerSize ) {
      this.blockSize = blockSize;
      this.className = className;
      this.headerSize = headerSize;
    }
  }

  private class ReadBlockOffset {
    public final long start;
    public final int length;

    public ReadBlockOffset( final long start , final int length ) {
      this.start = start;
      this.length = length;
    }
  }

  private FileHeaderMeta readFileHeader( final InputStream in ) throws IOException {
    byte[] magic = new byte[MAGIC.length];
    InputStreamUtils.read( in , magic , 0 , MAGIC.length );

    if ( ! Arrays.equals( magic , MAGIC) ) {
      throw new IOException( "Invalid binary." );
    }

    byte[] blockSizeBytes = new byte[Integer.BYTES];
    InputStreamUtils.read( in , blockSizeBytes , 0 , Integer.BYTES );
    ByteBuffer wrapBuffer = ByteBuffer.wrap( blockSizeBytes );
    final int readBlockSize = wrapBuffer.getInt( 0 );

    byte[] blockClassLength = new byte[Integer.BYTES];
    ByteBuffer wrapLengthBuffer = ByteBuffer.wrap( blockClassLength );
    InputStreamUtils.read( in , blockClassLength , 0 , Integer.BYTES );
    int classNameSize = wrapLengthBuffer.getInt( 0 );

    byte[] blockClass = new byte[classNameSize];
    InputStreamUtils.read( in , blockClass , 0 , classNameSize );
    ByteBuffer classNameBuffer = ByteBuffer.wrap( blockClass );
    CharBuffer viewCharBuffer = classNameBuffer.asCharBuffer();
    char[] classNameChars = new char[ classNameSize / Character.BYTES ];
    viewCharBuffer.get( classNameChars );
    String blockReaderClass = BlockReaderNameShortCut.getClassName( new String( classNameChars ) );

    return new FileHeaderMeta(
        readBlockSize , 
        blockReaderClass ,
        ( MAGIC.length + ( Integer.BYTES * 2 ) + classNameSize ) );
  }

  public void setBlockSkipIndex( final IExpressionNode blockSkipIndex ) {
    this.blockSkipIndex = blockSkipIndex;
  }

  public void setNewStream(
      final InputStream in , final long dataSize , final Configuration config ) throws IOException {
    setNewStream( in , dataSize , config , 0 , dataSize );
  }

  /**
   * Set InputStream of the file.
   */
  public void setNewStream(
      final InputStream in ,
      final long dataSize ,
      final Configuration config ,
      final long start ,
      final long length ) throws IOException {
    inReadOffset = 0;
    readTargetList.clear();

    this.in = in;

    FileHeaderMeta meta = readFileHeader( in );
    inReadOffset += meta.headerSize;
    if ( ! blockReaderMap.containsKey( meta.className ) ) {
      IBlockReader blockReader = (IBlockReader)(
          FindClass.getObject( meta.className , true , this.getClass().getClassLoader() ) );
      blockReaderMap.put( meta.className , blockReader );
    }

    currentBlockReader = blockReaderMap.get( meta.className );
    currentBlockReader.setup( config );
    currentBlockReader.setBlockSkipIndex( blockSkipIndex );

    blockSize = meta.blockSize;

    int blockCount = Double.valueOf( Math.ceil( (double)dataSize / (double)blockSize ) ).intValue();
    for ( int i = 0 ; i < blockCount ; i++ ) {
      int targetBlockSize = blockSize;
      if ( i == 0 ) {
        targetBlockSize -= meta.headerSize;
      }
      long readStartOffset = (long)i * (long)blockSize;
      if ( start <= readStartOffset && readStartOffset < ( start + length ) ) {
        readTargetList.add( new ReadBlockOffset( readStartOffset , targetBlockSize ) );
      }
    }
    if ( readTargetList.isEmpty() ) {
      return;
    }
    currentBlockReader.setBlockSize( blockSize );
    setNextBlock();
  }

  /**
   * It is judged whether there is the next Spread.
   */
  public boolean hasNext() throws IOException {
    return setNextBlock();
  }

  private boolean setNextBlock() throws IOException {
    while ( ! currentBlockReader.hasNext() ) {
      if ( readTargetList.isEmpty() ) {
        return false;
      }
      ReadBlockOffset readOffset = readTargetList.remove(0);
      inReadOffset += InputStreamUtils.skip( in , readOffset.start - inReadOffset );
      currentBlockReader.setStream( in , readOffset.length );
      inReadOffset += readOffset.length;
    }
    return true;
  }

  /**
   * Get the next Spread as a Spread.
   */
  public Spread next() throws IOException {
    if ( ! setNextBlock() ) {
      return new Spread();
    }
    return currentBlockReader.next();
  }

  /**
   * Get the next Spread as a list of ColumnBinary.
   */
  public List<ColumnBinary> nextRaw() throws IOException {
    if ( ! setNextBlock() ) {
      return new ArrayList<ColumnBinary>();
    }
    return currentBlockReader.nextRaw();
  }

  public int getBlockReadCount() {
    return currentBlockReader.getBlockReadCount();
  }

  public int getBlockCount() {
    return currentBlockReader.getBlockCount();
  }

  public long getReadPos() {
    return inReadOffset;
  }

  public Integer getCurrentSpreadSize() {
    return currentBlockReader.getCurrentSpreadSize();
  }

  public SummaryStats getReadStats() {
    return currentBlockReader.getReadStats();
  }

  /**
   * Close InputStream and reset internal data.
   */
  public void close() throws IOException {
    if ( in != null ) {
      in.close();
      in = null;
    }
    inReadOffset = 0;
    readTargetList.clear();
    currentBlockReader.close();
  }

}
