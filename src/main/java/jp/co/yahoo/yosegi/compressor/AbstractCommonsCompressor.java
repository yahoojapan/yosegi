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

package jp.co.yahoo.yosegi.compressor;

import jp.co.yahoo.yosegi.util.io.InputStreamUtils;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public abstract class AbstractCommonsCompressor implements ICompressor {

  abstract InputStream createInputStream( final InputStream in ) throws IOException;

  abstract OutputStream createOutputStream(
      final OutputStream out ,
      final long decompressSize,
      final CompressResult compressResult ) throws IOException;

  @Override
  public byte[] compress(
      final byte[] data ,
      final int start ,
      final int length ,
      final CompressResult compressResult ) throws IOException {
    ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
    OutputStream out = createOutputStream( byteArrayOut , length , compressResult );

    out.write( data , start , length );
    out.close();
    byte[] compressByte = byteArrayOut.toByteArray();
    byte[] retVal = new byte[ Integer.BYTES + compressByte.length ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( retVal );
    wrapBuffer.putInt( length );
    wrapBuffer.put( compressByte );

    byteArrayOut.close();
    compressResult.feedBack( length , compressByte.length );

    return retVal;
  }

  @Override
  public int getDecompressSize(
      final byte[] data , final int start , final int length ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap( data , start , length );
    return wrapBuffer.getInt();
  }

  @Override
  public byte[] decompress(
      final byte[] data , final int start , final int length ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap( data , start , length );
    int dataLength = wrapBuffer.getInt();

    ByteArrayInputStream byteArrayIn =
        new ByteArrayInputStream( data , start + Integer.BYTES , length );
    InputStream in = createInputStream( byteArrayIn );

    byte[] retVal = new byte[dataLength];
    InputStreamUtils.read( in , retVal , 0 , dataLength );
    in.close();

    return retVal;
  }

  @Override
  public int decompressAndSet(
      final byte[] data ,
      final int start ,
      final int length ,
      final byte[] buffer ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap( data , start , length );
    int dataLength = wrapBuffer.getInt();

    ByteArrayInputStream byteArrayIn =
        new ByteArrayInputStream( data , start + Integer.BYTES , length );
    InputStream in = createInputStream( byteArrayIn );

    InputStreamUtils.read( in , buffer , 0 , dataLength );
    in.close();

    return dataLength;
  }

}
