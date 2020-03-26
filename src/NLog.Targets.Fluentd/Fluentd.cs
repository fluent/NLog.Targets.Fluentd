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
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using MsgPack;
using MsgPack.Serialization;

namespace NLog.Targets
{
    internal class OrdinaryDictionarySerializer : MessagePackSerializer<IDictionary<string, object>>
    {
        private readonly SerializationContext embeddedContext;

        internal OrdinaryDictionarySerializer(SerializationContext ownerContext, SerializationContext embeddedContext) : base(ownerContext)
        {
            this.embeddedContext = embeddedContext ?? ownerContext;
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
                    packer.Pack(pair.Value, this.embeddedContext);
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
            var dictionary = collection as IDictionary<string, object>;
            if (dictionary == null)
                throw new NotSupportedException();
            UnpackTo(unpacker, dictionary);
        }
    }

    internal class FluentdEmitter
    {
        private readonly Packer packer;
        private readonly SerializationContext serializationContext;
        private readonly Stream destination;

        public void Emit(DateTime timestamp, string tag, IDictionary<string, object> data)
        {
            this.packer.PackArrayHeader(3);
            this.packer.PackString(tag, Encoding.UTF8);
            this.packer.PackEventTime(timestamp);
            this.packer.Pack(data, serializationContext);
            this.destination.Flush();    // Change to packer.Flush() when packer is upgraded
        }

        public FluentdEmitter(Stream stream)
        {
            this.destination = stream;

            // PackerCompatibilityOptions.ProhibitExtendedTypeObjects must be turned off in order to use the PackExtendedTypeValue method
            this.packer = Packer.Create(destination, Packer.DefaultCompatibilityOptions & ~PackerCompatibilityOptions.ProhibitExtendedTypeObjects);
            var embeddedContext = new SerializationContext(this.packer.CompatibilityOptions);
            embeddedContext.Serializers.Register(new OrdinaryDictionarySerializer(embeddedContext, null));
            this.serializationContext = new SerializationContext(PackerCompatibilityOptions.PackBinaryAsRaw);
            this.serializationContext.Serializers.Register(new OrdinaryDictionarySerializer(this.serializationContext, embeddedContext));
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

        private TcpClient client;

        private Stream stream;

        private FluentdEmitter emitter;

        protected override void InitializeTarget()
        {
            base.InitializeTarget();
        }

        private void InitializeClient()
        {
            this.client = new TcpClient();
            this.client.NoDelay = this.NoDelay;
            this.client.ReceiveBufferSize = this.ReceiveBufferSize;
            this.client.SendBufferSize = this.SendBufferSize;
            this.client.SendTimeout = this.SendTimeout;
            this.client.ReceiveTimeout = this.ReceiveTimeout;
            this.client.LingerState = new LingerOption(this.LingerEnabled, this.LingerTime);
        }

        protected void EnsureConnected()
        {
            if (this.client == null)
            {
                InitializeClient();
                ConnectClient();
            }
            else if (!this.client.Connected)
            {
                Cleanup();
                InitializeClient();
                ConnectClient();
            }
        }

        private void ConnectClient()
        {
            this.client.Connect(this.Host, this.Port);
            this.stream = this.client.GetStream();
            this.emitter = new FluentdEmitter(this.stream);
        }

        protected void Cleanup()
        {
            try
            {
                this.stream?.Dispose();
                this.client?.Close();
            }
            catch (Exception ex)
            {
                NLog.Common.InternalLogger.Warn("Fluentd Close - " + ex.ToString());
            }
            finally
            {
                this.stream = null;
                this.client = null;
                this.emitter = null;
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
            if (this.EmitStackTraceWhenAvailable && logEvent.HasStackTrace)
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
            if (this.IncludeAllProperties && logEvent.Properties.Count > 0)
            {
                foreach (var property in logEvent.Properties)
                {
                    var propertyKey = property.Key.ToString();
                    if (string.IsNullOrEmpty(propertyKey))
                        continue;

                    record[propertyKey] = SerializePropertyValue(propertyKey, property.Value);
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
                this.emitter?.Emit(logEvent.TimeStamp, this.Tag, record);
            }
            catch (Exception ex)
            {
                NLog.Common.InternalLogger.Warn("Fluentd Emit - " + ex.ToString());
                throw;  // Notify NLog of failure
            }
        }

        private static object SerializePropertyValue(string propertyKey, object propertyValue)
        {
            if (propertyValue == null || Convert.GetTypeCode(propertyValue) != TypeCode.Object || propertyValue is decimal)
            {
                return propertyValue;   // immutable
            }
            else
            {
                return propertyValue.ToString();
            }
        }

        public Fluentd()
        {
            this.Host = "127.0.0.1";
            this.Port = 24224;
            this.ReceiveBufferSize = 8192;
            this.SendBufferSize = 8192;
            this.ReceiveTimeout = 1000;
            this.SendTimeout = 1000;
            this.LingerEnabled = true;
            this.LingerTime = 1000;
            this.EmitStackTraceWhenAvailable = false;
            this.Tag = Assembly.GetCallingAssembly().GetName().Name;
        }
    }
}
