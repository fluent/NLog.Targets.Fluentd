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
using System.Linq;
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
        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private readonly Packer _packer;
        private readonly SerializationContext _serializationContext;

        public void Emit(DateTime timestamp, string tag, IDictionary<string, object> data)
        {
            var unixTimestamp = timestamp.ToUniversalTime().Subtract(UnixEpoch).Ticks / 10000000;
            _packer.PackArrayHeader(3);
            _packer.PackString(tag, Encoding.UTF8);
            _packer.Pack((ulong)unixTimestamp);
            _packer.Pack(data, _serializationContext);
        }

        public FluentdEmitter(Stream stream)
        {
            this._serializationContext = new SerializationContext(PackerCompatibilityOptions.PackBinaryAsRaw);
            this._serializationContext.Serializers.Register(new OrdinaryDictionarySerializer());
            this._packer = Packer.Create(stream);
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

        private TcpClient _client;

        private Stream _stream;

        private FluentdEmitter _emitter;

        protected override void InitializeTarget()
        {
            base.InitializeTarget();
            _client.NoDelay = this.NoDelay;
            _client.ReceiveBufferSize = this.ReceiveBufferSize;
            _client.SendBufferSize = this.SendBufferSize;
            _client.SendTimeout = this.SendTimeout;
            _client.ReceiveTimeout = this.ReceiveTimeout;
            _client.LingerState = new LingerOption(this.LingerEnabled, this.LingerTime);
        }

        protected void EnsureConnected()
        {
            try
            {
                if (_client.Connected) return;
                _client.Connect(this.Host, this.Port);
                this._stream = this._client.GetStream();
                this._emitter = new FluentdEmitter(this._stream);
            }
            catch (Exception e)
            {
            }
        }

        protected void Cleanup()
        {
            if (this._stream != null)
            {
                this._stream.Dispose();
                this._stream = null;
            }
            if (this._client != null)
            {
                this._client.Close();
                this._client = null;
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
                var stackTrace = logEvent.StackTrace;
                var transcodedFrames = stackTrace.GetFrames().Select(frame => new Dictionary<string, object>
                {
                    {"filename", frame.GetFileName()}, {"line", frame.GetFileLineNumber()}, {"column", frame.GetFileColumnNumber()}, {"method", frame.GetMethod().ToString()}, {"il_offset", frame.GetILOffset()}, {"native_offset", frame.GetNativeOffset()},
                }).ToList();
                record.Add("stacktrace", transcodedFrames);
            }
            EnsureConnected();
            if (_emitter == null) return;
            try
            {
                this._emitter.Emit(logEvent.TimeStamp, Tag, record);
            }
            catch (Exception e)
            {
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
            _client = new TcpClient();
        }
    }
}
