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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NLog;

namespace Demo
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new NLog.Config.LoggingConfiguration();
            using (var fluentdTarget = new NLog.Targets.Fluentd())
            {
                fluentdTarget.Layout = new NLog.Layouts.SimpleLayout("${message}");
                config.AddTarget("fluentd", fluentdTarget);
                config.LoggingRules.Add(new NLog.Config.LoggingRule("demo", LogLevel.Debug, fluentdTarget));
                var loggerFactory = new LogFactory(config);
                var logger = loggerFactory.GetLogger("demo");
                logger.Info("{\"Data\":\"this is test data\"}");
                logger.Info("Test Message");
            }
        }
    }
}
