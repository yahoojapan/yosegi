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

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.spread.ArrowSpreadUtil;
import jp.co.yahoo.yosegi.spread.Spread;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;

import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.List;

public class YosegiArrowWriter implements AutoCloseable {

  private final YosegiWriter writer;

  /**
   * Create new YosegiArrowWriter.
   */
  public static YosegiArrowWriter newInstance( final String localFilePath ) throws IOException {
    return newInstance( new File( localFilePath ) );
  }

  /**
   * Create new YosegiArrowWriter.
   */
  public static YosegiArrowWriter newInstance(
      final String localFilePath , final Configuration config ) throws IOException {
    return newInstance( new File( localFilePath ) , config );
  }

  /**
   * Create new YosegiArrowWriter.
   */
  public static YosegiArrowWriter newInstance(
      final File localFile ) throws IOException {
    return newInstance( localFile , new Configuration() );
  }

  /**
   * Create new YosegiArrowWriter.
   */
  public static YosegiArrowWriter newInstance(
      final File localFile , final Configuration config ) throws IOException {
    return new YosegiArrowWriter( new FileOutputStream( localFile ) , config );
  }

  public YosegiArrowWriter(
      final OutputStream out , final Configuration config ) throws IOException {
    writer = new YosegiWriter( out , config );
  }

  /**
   * Append from arrow byte array.
   */
  public void append( final byte[] buffer ) throws IOException {
    ArrowFileReader arrowReader = new ArrowFileReader(
        new SeekableInMemoryByteChannel( buffer ) , new RootAllocator( Integer.MAX_VALUE ) );
    List<ArrowBlock> blockList = arrowReader.getRecordBlocks();
    for ( ArrowBlock block : blockList ) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      arrowReader.loadRecordBatch(block);
      append( root );
    }
  }

  /**
   * Append from arrow byte array.
   */
  public void append(
      final byte[] buffer , final int start , final int length ) throws IOException {
    byte[] newBuffer = new byte[length];
    System.arraycopy( buffer , start , newBuffer , 0 , length );
    append( newBuffer );
  }

  public void append( final VectorSchemaRoot root ) throws IOException {
    Spread spread = ArrowSpreadUtil.toSpread( root );
    writer.append( spread );
  }

  /**
   * Close.
   */
  @Override
  public void close() throws IOException {
    writer.close();
  }

}
