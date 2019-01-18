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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface ICompressor {

  default byte[] compress(
      final byte[] data , final int start , final int length ) throws IOException {
    return compress( data , start , length , DataType.TEXT );
  }

  byte[] compress(
      final byte[] data ,
      final int start ,
      final int length ,
      final DataType dataType ) throws IOException;

  int getDecompressSize(
      final byte[] data , final int start , final int length ) throws IOException;

  byte[] decompress(
      final byte[] data , final int start , final int length ) throws IOException;

  int decompressAndSet(
      final byte[] data ,
      final int start ,
      final int length ,
      final byte[] buffer ) throws IOException;

}
