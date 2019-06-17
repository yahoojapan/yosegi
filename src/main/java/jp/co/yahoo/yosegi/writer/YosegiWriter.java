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

package jp.co.yahoo.yosegi.writer;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.block.FindBlockWriter;
import jp.co.yahoo.yosegi.block.IBlockWriter;
import jp.co.yahoo.yosegi.block.PushdownSupportedBlockWriter;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.spread.Spread;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.List;

public class YosegiWriter implements AutoCloseable {

  private static final byte[] MAGIC = new byte[]{'$','C','L','M'};

  private final OutputStream out;
  private final IBlockWriter blockMaker;

  /**
   * Initialize by setting OutputStream.
   */
  public YosegiWriter( final OutputStream out , final Configuration config ) throws IOException {
    this.out = out;

    int blockSize = config.getInt( "block.size" , 1024 * 1024 * 64 );

    blockMaker = FindBlockWriter.get(
        config.get( "block.maker.class" , PushdownSupportedBlockWriter.class.getName() ) );
    blockMaker.setup( blockSize , config );
    String blockMakerClassName = blockMaker.getReaderClassName();
    int classNameLength = blockMakerClassName.length() * Character.BYTES;

    byte[] header = new byte[MAGIC.length + Integer.BYTES + Integer.BYTES + classNameLength ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( header );
    final CharBuffer viewCharBuffer = wrapBuffer.asCharBuffer();
    int offset = 0;
    wrapBuffer.put( MAGIC , 0 , MAGIC.length );
    offset += MAGIC.length;
    wrapBuffer.putInt( offset , blockSize );
    offset += Integer.BYTES;
    wrapBuffer.putInt( offset , classNameLength );
    offset += Integer.BYTES;
    viewCharBuffer.position( offset / Character.BYTES );
    viewCharBuffer.put( blockMakerClassName.toCharArray() );

    blockMaker.appendHeader( header );
  }

  /**
   * Convert Spread to ColumnBinary.
   */
  public List<ColumnBinary> convertRow( final Spread spread ) throws IOException {
    return blockMaker.convertRow( spread );
  }

  /**
   * Add Spread as a Spread.
   */
  public void append( final Spread spread ) throws IOException {
    List<ColumnBinary> binaryList = blockMaker.convertRow( spread );
    appendRow(binaryList, spread.size());
  }

  /**
   * Add Spread as a ColumnBinary list.
   */
  public void appendRow(
      final List<ColumnBinary> binaryList, final int spreadSize ) throws IOException {
    if ( ! blockMaker.canAppend( binaryList ) ) {
      blockMaker.writeFixedBlock( out );
    }
    blockMaker.append( spreadSize , binaryList );
  }

  public void writeFixedBlock() throws IOException {
    blockMaker.writeFixedBlock( out );
  }

  /**
   * Close.
   */
  public void close() throws IOException {
    blockMaker.writeVariableBlock( out );
    blockMaker.close();
    out.close();
  }

  public IBlockWriter getBlockWriter() {
    return blockMaker;
  }
}
