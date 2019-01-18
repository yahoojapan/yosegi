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

package jp.co.yahoo.yosegi.message.parser.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jp.co.yahoo.yosegi.message.parser.IMessageReader;
import jp.co.yahoo.yosegi.message.parser.IParser;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;

public class JacksonMessageReader implements IMessageReader {

  private final ObjectMapper objectMapper = new ObjectMapper();

  public IParser create( final File file ) throws IOException {
    JsonNode rootNode = objectMapper.readTree( file );
    return JsonNodeToParser.get( rootNode );
  }

  public IParser create( final InputStream in ) throws IOException {
    JsonNode rootNode = objectMapper.readTree( in );
    return JsonNodeToParser.get( rootNode );
  }

  public IParser create( final Reader reader ) throws IOException {
    JsonNode rootNode = objectMapper.readTree( reader );
    return JsonNodeToParser.get( rootNode );
  }

  public IParser create( final String message ) throws IOException {
    JsonNode rootNode = objectMapper.readTree( message );
    return JsonNodeToParser.get( rootNode );
  }

  public IParser create( final URL url ) throws IOException {
    JsonNode rootNode = objectMapper.readTree( url );
    return JsonNodeToParser.get( rootNode );
  }

  @Override
  public IParser create( final byte[] message ) throws IOException {
    return create( message , 0 , message.length );
  }

  @Override
  public IParser create( final byte[] message ,
      final int start , final int length ) throws IOException {
    InputStream in = new ByteArrayInputStream( message , start , length );
    return create( in );
  }

}
