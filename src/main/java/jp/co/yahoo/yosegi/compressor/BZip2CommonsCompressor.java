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

import jp.co.yahoo.yosegi.util.EnumDispatcherFactory;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class BZip2CommonsCompressor extends AbstractCommonsCompressor {
  private static EnumDispatcherFactory.Func<CompressionPolicy, Integer> compressLevelDispatcher;

  static {
    compressLevelDispatcher = (new EnumDispatcherFactory<>(CompressionPolicy.class))
      .setDefault(6)
      .set(CompressionPolicy.BEST_SPEED, BZip2CompressorOutputStream.MIN_BLOCKSIZE)
      .set(CompressionPolicy.SPEED, 3)
      .set(CompressionPolicy.DEFAULT, 6)
      .set(CompressionPolicy.BEST_COMPRESSION, BZip2CompressorOutputStream.MAX_BLOCKSIZE)
      .create();
  }

  @Override
  public InputStream createInputStream( final InputStream in ) throws IOException {
    return new BZip2CompressorInputStream( in );
  }

  @Override
  public OutputStream createOutputStream(
      final OutputStream out , final CompressResult compressResult ) throws IOException {
    int level = compressLevelDispatcher.get( compressResult.getCompressionPolicy() );
    int optLevel = compressResult.getCurrentLevel();
    if ( ( level - optLevel ) < BZip2CompressorOutputStream.MIN_BLOCKSIZE ) {
      compressResult.setEnd();
      optLevel = compressResult.getCurrentLevel();
    }
    return new BZip2CompressorOutputStream( out , level - optLevel );
  }

}
