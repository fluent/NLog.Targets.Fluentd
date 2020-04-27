NLog.Targets.Fluentd
====================

NLog.Targets.Fluents is a custom target of [NLog](https://github.com/nlog/NLog) that emits the log entries to a [fluentd](http://www.fluentd.org/) node.

Settings
--------

Setting                     | Description                                                  | Example       
--------------------------- | -----------------------------------------------------------  | --------------
Host                        | Host name of the fluentd node                                | example.local
Port                        | Port number of the fluentd node                              | 24224
Tag                         | Fluentd tag name                                             | windowshost
NoDelay                     | Enable Nagle's algorithm                                     | true
SendBufferSize              | Send buffer size                                             | 8192
SendTimeout                 | Send timeout                                                 | 2
LingerEnabled               | Wait for all the data to be sent when closing the connection | false
LingerTime                  | Linger timeout in seconds (default: 1000)                    | 2
EmitStackTraceWhenAvailable | Emit a stacktrace for every log entry when available         | false
IncludeAllProperties        | Include structured logging parameters for every log entry    | false


License
-------

NLog.Targets.Fluentd

Copyright (c) 2014 Moriyoshi Koizumi and contributors.

This software is licensed under the Apache License, Version 2.0 (the "License");
you may not use this software except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
