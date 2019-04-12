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

import jp.co.yahoo.yosegi.util.INamePair;

public enum CompressorName implements INamePair {
  DEFAULT( "default",  "jp.co.yahoo.yosegi.compressor.DefaultCompressor"),
  GZIP(    "gzip",     "jp.co.yahoo.yosegi.compressor.GzipCompressor"),
  GZ(      "gz",       "jp.co.yahoo.yosegi.compressor.GzipCommonsCompressor"),
  DEFLATER("deflater", "jp.co.yahoo.yosegi.compressor.DeflateCommonsCompressor"),
  BZIP2(  "bzip2",     "jp.co.yahoo.yosegi.compressor.BZip2CommonsCompressor"),
  LZ4(    "lz4",       "jp.co.yahoo.yosegi.compressor.FramedLZ4CommonsCompressor"),
  SNAPPY( "snappy",    "jp.co.yahoo.yosegi.compressor.FramedSnappyCommonsCompressor"),
  LZMA(   "lzma",      "jp.co.yahoo.yosegi.compressor.LzmaCommonsCompressor"),
  ZSTD(   "zstd",      "jp.co.yahoo.yosegi.compressor.ZstdCommonsCompressor"),
  ;

  private final String shortName;
  private final String longName;

  private CompressorName(final String shortName, final String longName) {
    this.shortName = shortName;
    this.longName = longName;
  }

  public String getLongName() {
    return longName;
  }

  public String getShortName() {
    return shortName;
  }
}

