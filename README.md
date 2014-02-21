NLog.Targets.Fluentd
====================

NLog.Targets.Fluents is a custom target of [NLog](https://github.com/nlog/NLog) that emits the log entries to a [fluentd](http://www.fluentd.org/) node.

Settings
--------

Setting           | Description                                                  | Example       
----------------- | -----------------------------------------------------------  | --------------
Host              | Host name of the fluentd node                                | example.local
Port              | Port number of the fluentd node                              | 24224
Tag               | Fluentd tag name                                             | windowshost
NoDelay           | Enable Nagle's algorithm                                     | true
SendBufferSize    | Send buffer size                                             | 8192
SendTimeout       | Send timeout                                                 | 2
LingerEnabled     | Wait for all the data to be sent when closing the connection | false
LingerTime        | Linger timeout                                               | 2

