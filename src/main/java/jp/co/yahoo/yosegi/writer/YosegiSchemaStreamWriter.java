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
import jp.co.yahoo.yosegi.message.formatter.IStreamWriter;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class YosegiSchemaStreamWriter implements IStreamWriter {

  private final YosegiRecordWriter writer;

  public YosegiSchemaStreamWriter(
      final OutputStream out , final Configuration config ) throws IOException {
    writer = new YosegiRecordWriter( out ,config );
  }

  @Override
  public void write( final PrimitiveObject obj ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported write( final PrimitiveObject obj )" );
  }

  @Override
  public void write( final List<Object> array ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported write( final List<Object> array )" );
  }

  @Override
  public void write( final Map<Object,Object> map ) throws IOException {
    writer.addRow( (Map)map );
  }

  @Override
  public void write( final IParser parser ) throws IOException {
    writer.addParserRow( parser );
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

}
