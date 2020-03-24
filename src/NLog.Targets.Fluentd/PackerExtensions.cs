using System;

namespace NLog.Targets
{
    using MsgPack;

    internal static class PackerExtensions
    {
        private const int nanoSecondsPerSecond = 1 * 1000 * 1000 * 1000;
        private const double ticksToNanoSecondsFactor = (double)nanoSecondsPerSecond / TimeSpan.TicksPerSecond;
        private static readonly long unixEpochTicks = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

        /// <summary>
        /// Write according to Fluend EventTime specification.
        /// </summary>
        /// <remarks>
        /// Specification: https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#eventtime-ext-format"
        /// </remarks>
        public static Packer PackEventTime(this Packer that, DateTime value)
        {
            DateTimeToEpoch(value, out var secondsFromEpoch, out var nanoSeconds);

            that.PackExtendedTypeValue(
                0x0,
                new[]
                {
                    (byte) ((ulong) (secondsFromEpoch >> 24) & (ulong) byte.MaxValue),
                    (byte) ((ulong) (secondsFromEpoch >> 16) & (ulong) byte.MaxValue),
                    (byte) ((ulong) (secondsFromEpoch >> 8) & (ulong) byte.MaxValue),
                    (byte) ((ulong) secondsFromEpoch & (ulong) byte.MaxValue),
                    (byte) ((ulong) (nanoSeconds >> 24) & (ulong) byte.MaxValue),
                    (byte) ((ulong) (nanoSeconds >> 16) & (ulong) byte.MaxValue),
                    (byte) ((ulong) (nanoSeconds >> 8) & (ulong) byte.MaxValue),
                    (byte) ((ulong) nanoSeconds & (ulong) byte.MaxValue),
                });

            return that;
        }

        private static void DateTimeToEpoch(DateTime value, out uint secondsFromEpoch, out uint nanoSeconds)
        {
            var fromEpochTicks = value.ToUniversalTime().Ticks - unixEpochTicks;
            secondsFromEpoch = (uint)(fromEpochTicks / TimeSpan.TicksPerSecond);
            nanoSeconds = (uint)((fromEpochTicks - secondsFromEpoch * TimeSpan.TicksPerSecond) * ticksToNanoSecondsFactor);
        }
    }
}
