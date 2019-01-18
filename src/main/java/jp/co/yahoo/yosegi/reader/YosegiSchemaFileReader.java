/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.co.yahoo.yosegi.reader;

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.IStreamReader;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.xml.ws.WebServiceException;

public class YosegiSchemaFileReader implements Closeable,IStreamReader {

  private final YosegiSchemaReader reader;

  /**
   * Set the file to be read and initialize it.
   */
  public YosegiSchemaFileReader( final File file, final Configuration config) throws IOException {
    reader = new YosegiSchemaReader();
    InputStream in = new FileInputStream(file);
    reader.setNewStream(in, file.length(), config);
  }


  @Override
  public boolean hasNext() throws IOException {
    return reader.hasNext();
  }

  @Override
  public IParser next() throws IOException {
    return reader.next();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

}
