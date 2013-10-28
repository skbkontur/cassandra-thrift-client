using System;

using log4net;

namespace SKBKontur.Cassandra.CassandraClient
{
    internal class TimeStatistics
    {
        public TimeStatistics(string timeStatisticsTitle)
        {
            this.timeStatisticsTitle = timeStatisticsTitle;
            counts = new int[250];
            totalCount = 0;
            maxTime = 0;
        }

        public void AddTime(double milliseconds)
        {
            double quantile95;
            double currentMaxTime;
            lock(locker)
            {
                var d = Math.Max(milliseconds * 10, 1);
                var bin = Math.Min((int)Math.Round(Math.Log10(d) * 30), counts.Length - 1);
                counts[bin]++;
                totalCount++;
                maxTime = Math.Max(maxTime, milliseconds);
                currentMaxTime = maxTime;
                quantile95 = GetQuantile95();
            }
            if(milliseconds > quantile95 || Math.Abs(milliseconds - currentMaxTime) < 1e-3)
                logger.InfoFormat(timeStatisticsTitle + ". Long running request. Time={0} Quantile95={1} Maximum={2}", milliseconds, quantile95, currentMaxTime);
        }

        public void LogStatistics()
        {
            logger.InfoFormat(timeStatisticsTitle + ". Requests={0} Quantile95={1} Maximum={2}", totalCount, GetQuantile95(), maxTime);
        }

        private double GetQuantile95()
        {
            var index = (int)Math.Round(totalCount * 0.95);
            var count = 0;
            for(var i = 0; i < counts.Length; i++)
            {
                count += counts[i];
                if(count >= index)
                    return (int)Math.Round(Math.Pow(10, (double)i / 30) / 10);
            }
            return maxTime;
        }

        private readonly string timeStatisticsTitle;

        private int totalCount;
        private readonly int[] counts;
        private double maxTime;
        private readonly object locker = new object();
        private static readonly ILog logger = LogManager.GetLogger(typeof(TimeStatistics));
    }
}