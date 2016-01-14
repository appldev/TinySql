using System;
using System.Runtime.Caching;

namespace TinySql.Cache
{
    #region interface and base cache classes

    public interface ICacheProvider<TKey, TValue>
    {
        TValue Get(TKey key);
        void Add(TKey key, TValue value);
        bool Remove(TKey key);

        bool IsCached(TKey key);

        int CacheMinutes { get; set; }
        bool UseSlidingCache { get; set; }

    }

    public abstract class ResultCacheProvider : ICacheProvider<SqlBuilder,ResultTable>
    {
        public ResultCacheProvider()
        {
            SetCachePolicy();
        }

        protected void SetCachePolicy()
        {
            if (UseSlidingCache)
            {
                _cachePolicy = new CacheItemPolicy() { AbsoluteExpiration = ObjectCache.InfiniteAbsoluteExpiration, SlidingExpiration = TimeSpan.FromMinutes(CacheMinutes) };
            }
            else
            {
                _cachePolicy = new CacheItemPolicy() { AbsoluteExpiration = DateTimeOffset.Now.AddMinutes(CacheMinutes), SlidingExpiration = ObjectCache.NoSlidingExpiration };
            }
        }

        private string GetKey(SqlBuilder builder)
        {
            return builder.ToSql().GetHashCode().ToString();
        }

        private CacheItemPolicy _cachePolicy = null;



        public ResultTable Get(SqlBuilder builder)
        {
            string key = GetKey(builder);
            if (MemoryCache.Default.Contains(key))
            {
                return (ResultTable)MemoryCache.Default.Get(key);
            }
            else
            {
                return null;
            }
        }

        public void Add(SqlBuilder builder, ResultTable result)
        {
            string key = GetKey(builder);
            CacheItem item = new CacheItem(key, result);
            MemoryCache.Default.Add(item, _cachePolicy);
        }

        public bool Remove(SqlBuilder builder)
        {
            try
            {
                string key = GetKey(builder);
                MemoryCache.Default.Remove(key);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public bool IsCached(SqlBuilder builder)
        {
            string key = GetKey(builder);
            return MemoryCache.Default.Contains(key);
        }

        private int _cacheMinutes = 2;
        public int CacheMinutes
        {
            get
            {
                return _cacheMinutes;
            }
            set
            {
                _cacheMinutes = value;
            }
        }

        private bool _useSlidingCache = true;

        public bool UseSlidingCache
        {
            get
            {
                return _useSlidingCache;
            }
            set
            {
                _useSlidingCache = value;
            }
        }
    }

    

    

    #endregion



    #region Default providers

    public class DefaultResultCacheProvider : ResultCacheProvider
    {

    }


    #endregion


    public sealed class CacheProvider
    {
        #region ctor
       
        private static CacheProvider _instance = null;
        private CacheProvider()
        {

        }

        public static CacheProvider Default
        {
            get
            {
                if (_instance == null)
                {
                    _instance = new CacheProvider();
                }
                return _instance;
            }
        }
        #endregion
        
        #region Result cache

        private static bool _useResultCache = false;

        public static bool UseResultCache
        {
            get { return _useResultCache; }
            set { _useResultCache = value; }
        }

        private static ResultCacheProvider _resultCacheInstance = null;
        public static ResultCacheProvider ResultCache
        {
            get
            {
                if (!UseResultCache)
                {
                    throw new InvalidOperationException("The Result Cache is not online. Set 'UseResultCache' to 'true'");
                }

                if (_resultCacheInstance == null)
                {
                    _resultCacheInstance = new DefaultResultCacheProvider();
                }

                return _resultCacheInstance;
            }
            set
            {
                _resultCacheInstance = value;
            }
        }

        #endregion

    }







}
