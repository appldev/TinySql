using System;
using System.Collections.Concurrent;
using System.IO;
using TinySql;
using TinySql.Metadata;
using TinySql.Serialization;

namespace UnitTests
{
    public static class StopWatch
    {
        public enum WatchTypes
        {
            Milliseconds,
            Seconds,
            Minues
        }
        private static ConcurrentDictionary<Guid, DateTime> Watches = new ConcurrentDictionary<Guid, DateTime>();
        public static Guid Start()
        {
            DateTime dt = DateTime.Now;
            Guid g = Guid.NewGuid();
            Watches.AddOrUpdate(g, dt, (k, v) => { return dt; });
            return g;
        }
        public static double Stop(Guid g, WatchTypes WatchType = WatchTypes.Milliseconds)
        {
            DateTime dt = DateTime.Now;
            DateTime st = Watches[g];
            DateTime r = DateTime.Now;
            Watches.TryRemove(g, out r);
            switch (WatchType)
            {
                case WatchTypes.Seconds:
                    return (dt - st).Seconds;
                case WatchTypes.Minues:
                    return (dt - st).Minutes;
            }
            return (dt - st).TotalMilliseconds;
        }
        public static string Stop(Guid g, WatchTypes WatchType = WatchTypes.Milliseconds, string Format = "{0}")
        {
            return GetWatch(g, true, WatchType, Format);
        }

        private static string GetWatch(Guid g, bool RemoveWatch, WatchTypes WatchType = WatchTypes.Milliseconds, string Format = "{0}")
        {
            DateTime dt = DateTime.Now;
            DateTime st = Watches[g];
            if (RemoveWatch)
            {
                DateTime r = DateTime.Now;
                Watches.TryRemove(g, out r);
            }
            double d = (dt - st).TotalMilliseconds;
            switch (WatchType)
            {
                case WatchTypes.Seconds:
                    d = (dt - st).TotalSeconds;
                    break;
                case WatchTypes.Minues:
                    d = (dt - st).TotalMinutes;
                    break;
            }
            return string.Format(Format, d);
        }

        public static string Split(Guid g, WatchTypes WatchType = WatchTypes.Milliseconds, string Format = "{0}")
        {
            return GetWatch(g, false, WatchType, Format);
        }


    }
    public static class SetupData
    {
        /// <summary>
        /// Requires the Adventure Works Sample Database for SQL 2014: https://msftdbprodsamples.codeplex.com/releases/view/125550
        /// </summary>
        /// <returns></returns>
        public static bool Setup()
        {
            if (SqlBuilder.DefaultConnection == null)
            {
                SqlBuilder.DefaultConnection = "Data Source=(localdb)\\ProjectsV12;Initial Catalog=TinyCrm;Integrated Security=True";
            }
            if (SqlBuilder.DefaultMetadata == null)
            {
                if (!File.Exists(_MetadataFileName))
                {
                    MetadataDatabase mdb = SqlMetadataDatabase.FromConnection(SqlBuilder.DefaultConnection, true).BuildMetadata();
                    mdb.ToFile(_MetadataFileName);
                    SqlBuilder.DefaultMetadata = mdb;
                }
                else
                {
                    SqlBuilder.DefaultMetadata = SerializationExtensions.FromFile(_MetadataFileName);
                }
            }

            // General options for SqlBuilder


            //string dir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            //SqlBuilder.DefaultConnection = string.Format("Server=(localdb)\\ProjectsV12;Database=TinyCrm;Trusted_Connection=Yes", dir);
            return true;
        }

        private static string _MetadataFileName = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "TinySql", "TinyCrm.json");
        public static string MetadataFileName
        {
            get
            {
                if (!File.Exists(_MetadataFileName))
                {
                    MetadataDatabase mdb = SqlMetadataDatabase.FromConnection(SqlBuilder.DefaultConnection, true).BuildMetadata();
                    mdb.ToFile(_MetadataFileName);
                    SqlBuilder.DefaultMetadata = mdb;
                }
                return _MetadataFileName;
            }
        }



    }
}
