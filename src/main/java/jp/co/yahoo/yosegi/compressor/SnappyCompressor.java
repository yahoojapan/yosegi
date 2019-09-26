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

import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SnappyCompressor implements ICompressor {

  @Override
  public byte[] compress(
      final byte[] data ,
      final int start ,
      final int length ,
      final CompressResult compressResult ) throws IOException {
    byte[] compressTarget;
    if ( start != 0 ) {
      compressTarget = new byte[length];
      System.arraycopy( data , start , compressTarget , 0 , length );
    } else {
      compressTarget = data;
    }

    byte[] compressByte = Snappy.rawCompress( compressTarget , length );
    byte[] retVal = new byte[ Integer.BYTES + compressByte.length ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( retVal );
    wrapBuffer.putInt( length );
    wrapBuffer.put( compressByte );

    compressResult.feedBack( length , compressByte.length );

    return retVal;
  }

  @Override
  public int getDecompressSize(
      final byte[] data , final int start , final int length ) throws IOException {
    return ByteBuffer.wrap( data , start , length ).getInt();
  }

  @Override
  public byte[] decompress(
      final byte[] data , final int start , final int length ) throws IOException {
    int dataLength = ByteBuffer.wrap( data , start , length ).getInt();
    byte[] retVal = new byte[dataLength];
    int size = Snappy.rawUncompress(
        data , start + Integer.BYTES , length - Integer.BYTES , retVal , 0 );
    if ( size != dataLength ) {
      throw new IOException( "Broken data." );
    }
    return retVal;
  }

  @Override
  public int decompressAndSet(
      final byte[] data ,
      final int start ,
      final int length ,
      final byte[] buffer ) throws IOException {
    byte[] decompressBinary = decompress( data , start , length );
    ByteBuffer.wrap( buffer ).put( decompressBinary );

    return decompressBinary.length;
  }

}
