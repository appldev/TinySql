namespace TinySql.UI
{
    public sealed class StringMap
    {
        public static StringMapStorageProvider StorageProvider {private get; set;}

        public StringMapStorageProvider InstanceStorageProvider
        {
            get;
            private set;
        }

        private static StringMap instance = null;
        public static StringMap Default
        {
            get
            {
                if (instance == null)
                {
                    instance = new StringMap();
                    instance.InstanceStorageProvider = StorageProvider;
                    // instance.InstanceStorageProvider.Load();
                }
                return instance;
            }
        }

        private void Load()
        {
            InstanceStorageProvider.Load();
        }
        public void Reset()
        {
            InstanceStorageProvider.Persist();
            instance = null;
        }

        public string GetText(int lcid, string key, string defaultValue = null)
        {
            // TODO: Implement Stringmap
            return defaultValue;
        }

    }

    public abstract class StringMapStorageProvider
    {
        public abstract void Persist();
        public abstract void Load();
    }


}
