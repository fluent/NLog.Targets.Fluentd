// NLog.Targets.Fluentd
// 
// Copyright (c) 2014 Moriyoshi Koizumi
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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

namespace NLog.Targets
{
    internal class OrdinaryDictionarySerializer: MessagePackSerializer<IDictionary<string, object>>
    {
        protected override void PackToCore(Packer packer, IDictionary<string, object> objectTree)
        {
            packer.PackMapHeader(objectTree);
            foreach (KeyValuePair<string, object> pair in objectTree)
            {
                packer.PackString(pair.Key);
                var serializationContext = new SerializationContext(packer.CompatibilityOptions);
                serializationContext.Serializers.Register(this);
                packer.Pack(pair.Value, serializationContext);
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
                if (unpacker.IsMapHeader)
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
        private Packer packer;
        private SerializationContext serializationContext;

        public void Emit(DateTime timestamp, string tag, IDictionary<string, object> data)
        {
            long unixTimestamp = timestamp.Subtract(unixEpoch).Ticks / 10000000;
            packer.PackArrayHeader(3);
            packer.PackString(tag, Encoding.UTF8);
            packer.Pack((ulong)unixTimestamp);
            packer.Pack(data, serializationContext);
        }

        public FluentdEmitter(Stream stream)
        {
            this.serializationContext = new SerializationContext(PackerCompatibilityOptions.PackBinaryAsRaw);
            this.serializationContext.Serializers.Register(new OrdinaryDictionarySerializer());
            this.packer = Packer.Create(stream);
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

        private TcpClient client;

        private Stream stream;

        private FluentdEmitter emitter;

        protected override void InitializeTarget()
        {
            base.InitializeTarget();
            client.NoDelay = this.NoDelay;
            client.ReceiveBufferSize = this.ReceiveBufferSize;
            client.SendBufferSize = this.SendBufferSize;
            client.SendTimeout = this.SendTimeout;
            client.ReceiveTimeout = this.ReceiveTimeout;
            client.LingerState = new LingerOption(this.LingerEnabled, this.LingerTime);
        }

        protected void EnsureConnected()
        {
            try
            {
                if (!client.Connected)
                {
                    client.Connect(this.Host, this.Port);
                    this.stream = this.client.GetStream();
                    this.emitter = new FluentdEmitter(this.stream);
                }
            }
            catch (Exception e)
            {
            }
        }

        protected void Cleanup()
        {
            if (this.stream != null)
            {
                this.stream.Dispose();
                this.stream = null;
            }
            if (this.client != null)
            {
                this.client.Close();
                this.client = null;
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
            EnsureConnected();
            if (this.emitter != null)
            {
                try
                {
                    this.emitter.Emit(logEvent.TimeStamp, Tag, record);
                }
                catch (Exception e)
                {
                }
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
            client = new TcpClient();
        }
    }
}
