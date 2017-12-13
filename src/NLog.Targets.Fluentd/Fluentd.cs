// NLog.Targets.Fluentd
// 
// Copyright (c) 2014 Moriyoshi Koizumi and contributors.
// 
// This file is licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//    http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Net.Sockets;
using System.Diagnostics;
using System.Reflection;
using NLog;
using MsgPack;
using MsgPack.Serialization;
using NLog.Common;

namespace NLog.Targets
{
    internal class OrdinaryDictionarySerializer : MessagePackSerializer<IDictionary<string, object>>
    {
        readonly SerializationContext _embeddedContext;

        internal OrdinaryDictionarySerializer(SerializationContext ownerContext, SerializationContext embeddedContext) : base(ownerContext)
        {
            _embeddedContext = embeddedContext ?? ownerContext;
        }

        protected override void PackToCore(Packer packer, IDictionary<string, object> objectTree)
        {
            packer.PackMapHeader(objectTree);
            foreach (KeyValuePair<string, object> pair in objectTree)
            {
                packer.PackString(pair.Key);
                if (pair.Value == null)
                {
                    packer.PackNull();
                }
                else
                {
                    packer.Pack(pair.Value, _embeddedContext);
                }
            }
        }

        protected void UnpackTo(Unpacker unpacker, IDictionary<string, object> dict, long mapLength)
        {
            for (long i = 0; i < mapLength; i++)
            {
                string key;
                MessagePackObject value;
                if (!unpacker.ReadString(out key))
                {
                    throw new InvalidMessagePackStreamException("string expected for a map key");
                }
                if (!unpacker.ReadObject(out value))
                {
                    throw new InvalidMessagePackStreamException("unexpected EOF");
                }
                if (unpacker.LastReadData.IsNil)
                {
                    dict.Add(key, null);
                }
                else if (unpacker.IsMapHeader)
                {
                    long innerMapLength = value.AsInt64();
                    var innerDict = new Dictionary<string, object>();
                    UnpackTo(unpacker, innerDict, innerMapLength);
                    dict.Add(key, innerDict);
                }
                else if (unpacker.IsArrayHeader)
                {
                    long innerArrayLength = value.AsInt64();
                    var innerArray = new List<object>();
                    UnpackTo(unpacker, innerArray, innerArrayLength);
                    dict.Add(key, innerArray);
                }
                else
                {
                    dict.Add(key, value.ToObject());
                }
            }
        }

        protected void UnpackTo(Unpacker unpacker, IList<object> array, long arrayLength)
        {
            for (long i = 0; i < arrayLength; i++)
            {
                MessagePackObject value;
                if (!unpacker.ReadObject(out value))
                {
                    throw new InvalidMessagePackStreamException("unexpected EOF");
                }
                if (unpacker.IsMapHeader)
                {
                    long innerMapLength = value.AsInt64();
                    var innerDict = new Dictionary<string, object>();
                    UnpackTo(unpacker, innerDict, innerMapLength);
                    array.Add(innerDict);
                }
                else if (unpacker.IsArrayHeader)
                {
                    long innerArrayLength = value.AsInt64();
                    var innerArray = new List<object>();
                    UnpackTo(unpacker, innerArray, innerArrayLength);
                    array.Add(innerArray);
                }
                else
                {
                    array.Add(value.ToObject());
                }
            }
        }

        public void UnpackTo(Unpacker unpacker, IDictionary<string, object> collection)
        {
            long mapLength;
            if (!unpacker.ReadMapLength(out mapLength))
            {
                throw new InvalidMessagePackStreamException("map header expected");
            }
            UnpackTo(unpacker, collection, mapLength);
        }

        protected override IDictionary<string, object> UnpackFromCore(Unpacker unpacker)
        {
            if (!unpacker.IsMapHeader)
            {
                throw new InvalidMessagePackStreamException("map header expected");
            }

            var retval = new Dictionary<string, object>();
            UnpackTo(unpacker, retval);
            return retval;
        }

        public void UnpackTo(Unpacker unpacker, object collection)
        {
            var _collection = collection as IDictionary<string, object>;
            if (_collection == null)
                throw new NotSupportedException();
            UnpackTo(unpacker, _collection);
        }
    }

    internal class FluentdEmitter
    {
        private static DateTime unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private readonly Packer _packer;
        private readonly SerializationContext _serializationContext;
        private readonly Stream _destination;

        public void Emit(DateTime timestamp, string tag, IDictionary<string, object> data)
        {
            long unixTimestamp = timestamp.ToUniversalTime().Subtract(unixEpoch).Ticks / 10000000;
            _packer.PackArrayHeader(3);
            _packer.PackString(tag, Encoding.UTF8);
            _packer.Pack((ulong)unixTimestamp);
            _packer.Pack(data, _serializationContext);
            _destination.Flush();    // Change to packer.Flush() when packer is upgraded
        }

        public FluentdEmitter(Stream stream)
        {
            _destination = stream;
            _packer = Packer.Create(_destination);
            var embeddedContext  = new SerializationContext(_packer.CompatibilityOptions);
            embeddedContext.Serializers.Register(new OrdinaryDictionarySerializer(embeddedContext, null));
            _serializationContext = new SerializationContext(PackerCompatibilityOptions.PackBinaryAsRaw);
            _serializationContext.Serializers.Register(new OrdinaryDictionarySerializer(_serializationContext, embeddedContext));
        }
    }

    [Target("Fluentd")]
    public class Fluentd : NLog.Targets.TargetWithLayout
    {
        public string Host { get; set; }

        public int Port { get; set; }

        public string Tag { get; set; }

        public bool NoDelay { get; set; }

        public int ReceiveBufferSize { get; set; }

        public int SendBufferSize { get; set; }

        public int SendTimeout { get; set; }

        public int ReceiveTimeout { get; set; }

        public bool LingerEnabled { get; set; }

        public int LingerTime { get; set; }

        public bool EmitStackTraceWhenAvailable { get; set; }

        public bool IncludeAllProperties { get; set; }

        private TcpClient _client;

        private Stream _stream;

        private FluentdEmitter _emitter;

        protected override void InitializeTarget()
        {
            base.InitializeTarget();
        }

        private void InitializeClient()
        {
            _client = new TcpClient();
            _client.NoDelay = NoDelay;
            _client.ReceiveBufferSize = ReceiveBufferSize;
            _client.SendBufferSize = SendBufferSize;
            _client.SendTimeout = SendTimeout;
            _client.ReceiveTimeout = ReceiveTimeout;
            _client.LingerState = new LingerOption(LingerEnabled, LingerTime);
        }

        protected void EnsureConnected()
        {
            if (_client == null)
            {
                InitializeClient();
                ConnectClient();
            }
            else if (!_client.Connected)
            {
                Cleanup();
                InitializeClient();
                ConnectClient();
            }
        }

        private void ConnectClient()
        {
            _client.Connect(Host, Port);
            _stream = _client.GetStream();
            _emitter = new FluentdEmitter(_stream);
        }

        protected void Cleanup()
        {
            try
            {
                _stream?.Dispose();
                _client?.Close();
            }
            catch (Exception ex)
            {
                NLog.Common.InternalLogger.Warn("Fluentd Close - " + ex.ToString());
            }
            finally
            {
                _stream = null;
                _client = null;
                _emitter = null;
            }
        }

        protected override void Dispose(bool disposing)
        {
            Cleanup();
            base.Dispose(disposing);
        }

        protected override void CloseTarget()
        {
            Cleanup();
            base.CloseTarget();
        }

        protected override void Write(LogEventInfo logEvent)
        {
            var record = new Dictionary<string, object> {
                { "level", logEvent.Level.Name },
                { "message", Layout.Render(logEvent) },
                { "logger_name", logEvent.LoggerName },
                { "sequence_id", logEvent.SequenceID },
            };
            if (EmitStackTraceWhenAvailable && logEvent.HasStackTrace)
            {
                var transcodedFrames = new List<Dictionary<string, object>>();
                StackTrace stackTrace = logEvent.StackTrace;
                foreach (StackFrame frame in stackTrace.GetFrames())
                {
                    var transcodedFrame = new Dictionary<string, object>
                    {
                        { "filename", frame.GetFileName() },
                        { "line", frame.GetFileLineNumber() },
                        { "column", frame.GetFileColumnNumber() },
                        { "method", frame.GetMethod().ToString() },
                        { "il_offset", frame.GetILOffset() },
                        { "native_offset", frame.GetNativeOffset() },
                    };
                    transcodedFrames.Add(transcodedFrame);
                }
                record.Add("stacktrace", transcodedFrames);
            }
            if (IncludeAllProperties && logEvent.Properties.Count > 0)
            {
                foreach (var property in logEvent.Properties)
                {
                    var propertyKey = property.Key.ToString();
                    if (string.IsNullOrEmpty(propertyKey))
                        continue;

                    record[propertyKey] = property.Value;
                }
            }

            try
            {
                EnsureConnected();
            }
            catch (Exception ex)
            {
                NLog.Common.InternalLogger.Warn("Fluentd Connect - " + ex.ToString());
                throw;  // Notify NLog of failure
            }

            try
            {
                _emitter?.Emit(logEvent.TimeStamp, Tag, record);
            }
            catch (Exception ex)
            {
                NLog.Common.InternalLogger.Warn("Fluentd Emit - " + ex.ToString());
                throw;  // Notify NLog of failure
            }
        }

        public Fluentd()
        {
            Host = "127.0.0.1";
            Port = 24224;
            ReceiveBufferSize = 8192;
            SendBufferSize = 8192;
            ReceiveTimeout = 1000;
            SendTimeout = 1000;
            LingerEnabled = true;
            LingerTime = 1000;
            EmitStackTraceWhenAvailable = false;
            Tag = Assembly.GetCallingAssembly().GetName().Name;
        }
    }
}
