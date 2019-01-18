<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
# Java Quick Start
This document explains the generation and reading of Yosegi file with a simple example.
It also explains the serialization and deserialization of messages.

## Deserializing messages
Yosegi converts the format of the input message into an object called IParser.
The following code is an example of converting from JSON to IParser.
In this example, JSON's character string is input by JacksonMessageReader, and IParser is acquired.

```
import java.io.IOException;

import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;

public class JavaQuickStart{

  public static void main( final String[] args ) throws IOException{
    String[] jsonMessages = new String[]{
      "{\"col1\":100,\"col2\":\"aaa\"}",
      "{\"col1\":200,\"col2\":\"bbb\"}",
      "{\"col1\":300,\"col2\":\"ccc\"}"
    };

    JacksonMessageReader reader = new JacksonMessageReader();
    for( String json : jsonMessages ){
      IParser jsonParser = reader.create( json );
    }
  }

}
```

## Generate Yosegi file
Yosegi has several Writer which generate files.
This example is an example of message unit input.

```
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    YosegiRecordWriter writer = new YosegiRecordWriter( out );

    for( String json : jsonMessages ){
      IParser jsonParser = jsonReader.create( json );
      writer.addParserRow( jsonParser );
    }
    writer.close();
```

The class that import is required is as follows.

```
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.writer.YosegiRecordWriter;
```

Please be sure to close.
If you do not close, exit without flushing the buffer.

## Serializing messages

Yosegi has a function to deserialize messages into various formats.
Deserialization is done by converting it to an object of IParser.

I will show you how to get it as a JSON message when reading from a file in the next session.

## Reading Yosegi file
There are several Readers to read Yosegi files.

In this example, we use Reader to read in message units.
Messages are read by IParser. Deserialize this message to JSON and output it.

InputStream is set to Reader. In this example, the ByteArrayOutputStream created earlier is the binary of the Yosegi file.
In this example it sets it to ByteArrayInputStream.

```
    byte[] yosegiFileByteArray = out.toByteArray();
    InputStream in = new ByteArrayInputStream( yosegiFileByteArray );
    YosegiSchemaReader reader = new YosegiSchemaReader();
    reader.setNewStream( in , yosegiFileByteArray.length , new Configuration() );
    JacksonMessageWriter jsonWriter = new JacksonMessageWriter();
    while( reader.hasNext() ){
      IParser message = reader.next();
      byte[] jsonByteArray = jsonWriter.create( message );
      System.out.println( new String( jsonByteArray ) );
    }
```

The class that import is required is as follows.

```
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.reader.YosegiSchemaReader;
import jp.co.yahoo.yosegi.message.formatter.json.JacksonMessageWriter;
```

When this program is executed, the output is as follows.

```
{"col2":"aaa","col1":100}
{"col2":"bbb","col1":200}
{"col2":"ccc","col1":300}
```

# What if I want to know more?

