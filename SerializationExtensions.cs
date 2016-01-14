using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using System;
using System.IO;
using System.Text;
using TinySql.Metadata;

namespace TinySql.Serialization
{
    public enum SerializerFormats
    {
        Json = 1,
        Bson = 2
    }
    public static class SerializationExtensions
    {
        private static JsonSerializerSettings Settings
        {
            get
            {
                return new JsonSerializerSettings()
                {
                    PreserveReferencesHandling = PreserveReferencesHandling.Objects,
                    ReferenceLoopHandling = ReferenceLoopHandling.Serialize,
                    TypeNameAssemblyFormat = System.Runtime.Serialization.Formatters.FormatterAssemblyStyle.Simple,
                    TypeNameHandling = TypeNameHandling.Objects,
                    DateFormatHandling = DateFormatHandling.IsoDateFormat,
                    Culture = SqlBuilder.DefaultCulture

                };
            }
        }

        public static T FromJson<T>(string json)
        {
            using (StringReader sr = new StringReader(json))
            using (JsonTextReader jr = new JsonTextReader(sr))
            {
                JsonSerializer serializer = JsonSerializer.Create(Settings);
                return serializer.Deserialize<T>(jr);
            }
        }

        public static byte[] ToBson<T>(T Object)
        {
            using (MemoryStream ms = new MemoryStream())
            using (BsonWriter bw = new BsonWriter(ms))
            {
                JsonSerializer serializer = JsonSerializer.Create(Settings);
                serializer.Serialize(bw, Object, typeof(T));
                return ms.ToArray();
            }
        }

        public static string ToJson<T>(T Object, bool formatOutput = false)
        {
            StringBuilder sb = new StringBuilder();
            using (StringWriter sw = new StringWriter(sb))
            using (JsonWriter jw = new JsonTextWriter(sw))
            {
                jw.Formatting = formatOutput ? Formatting.Indented : Formatting.None;
                JsonSerializer serializer = JsonSerializer.Create(Settings);
                serializer.Serialize(jw, Object);
                sw.Flush();
            }
            return sb.ToString();
        }

        public static void ToFile<T>(T Object, string fileName, bool createDirectory = true, bool formatOutput = false, SerializerFormats fileFormat = SerializerFormats.Json)
        {
            if (fileName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
            {
                fileFormat = SerializerFormats.Json;
            }
            else if (fileName.EndsWith(".bson", StringComparison.OrdinalIgnoreCase))
            {
                fileFormat = SerializerFormats.Bson;
            }
            string ext = fileFormat == SerializerFormats.Json ? "json" : "bson";
            if (createDirectory)
            {
                if (!Directory.Exists(Path.GetDirectoryName(fileName)))
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(fileName));
                }
            }
            if (!Path.GetExtension(fileName).ToLower().EndsWith(ext))
            {
                fileName += ext;
            }
            JsonSerializer serializer = JsonSerializer.Create(Settings);
            if (fileFormat == SerializerFormats.Json)
            {
                using (FileStream fs = File.OpenWrite(fileName))
                using (StreamWriter sw = new StreamWriter(fs))
                using (JsonWriter jw = new JsonTextWriter(sw))
                {
                    jw.Formatting = formatOutput ? Formatting.Indented : Formatting.None;
                    serializer.Serialize(jw, Object);
                }
            }
            else
            {
                using (MemoryStream ms = new MemoryStream())
                using (BsonWriter bw = new BsonWriter(ms))
                {
                    serializer.Serialize(bw, Object, typeof(T));
                    File.WriteAllBytes(fileName, ms.ToArray());
                }
            }
        }

        public static T FromFile<T>(string fileName, SerializerFormats? fileFormat = null)
        {
            if (fileFormat == null)
            {
                if (fileName.EndsWith(".bson", StringComparison.OrdinalIgnoreCase))
                {
                    fileFormat = SerializerFormats.Bson;
                }
                else if (fileName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
                {
                    fileFormat = SerializerFormats.Json;
                }
                else
                {
                    throw new ArgumentException("The file format cannot be infered from the file name. Set the FileFormat parameter", "fileFormat");
                }
            }
            JsonSerializerSettings settings = new JsonSerializerSettings()
            {
                PreserveReferencesHandling = PreserveReferencesHandling.All
            };

            using (FileStream fs = File.OpenRead(fileName))
            {
                JsonSerializer serializer = JsonSerializer.Create(settings);
                using (StreamReader sr = new StreamReader(fs))
                {
                    if (fileFormat.Value == SerializerFormats.Json)
                    {
                        using (JsonTextReader jr = new JsonTextReader(sr))
                        {
                            return serializer.Deserialize<T>(jr);
                        }
                    }
                    else
                    {
                        using (BsonReader br = new BsonReader(fs))
                        {
                            return serializer.Deserialize<T>(br);
                        }
                    }
                }
            }
        }



        #region Metadata

        public static void ToFile(this MetadataDatabase metadata, string fileName, bool createDirectory = true)
        {

            if (createDirectory)
            {
                if (!Directory.Exists(Path.GetDirectoryName(fileName)))
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(fileName));
                }
            }
            if (!Path.GetExtension(fileName).ToLower().EndsWith(".json"))
            {
                fileName += ".json";
            }
            using (FileStream fs = File.OpenWrite(fileName))
            using (StreamWriter sw = new StreamWriter(fs))
            using (JsonWriter jw = new JsonTextWriter(sw))
            {
                jw.Formatting = Formatting.None;
                JsonSerializer serializer = JsonSerializer.Create(Settings);
                serializer.Serialize(jw, metadata);
            }
        }
        public static MetadataDatabase FromFile(string fileName)
        {
            JsonSerializerSettings settings = new JsonSerializerSettings()
            {
                PreserveReferencesHandling = PreserveReferencesHandling.All
            };
            if (!Path.GetExtension(fileName).ToLower().EndsWith(".json"))
            {
                fileName += ".json";
            }
            using (FileStream fs = File.OpenRead(fileName))
            using (StreamReader sr = new StreamReader(fs))
            using (JsonTextReader jr = new JsonTextReader(sr))
            {
                JsonSerializer serializer = JsonSerializer.Create(settings);
                return serializer.Deserialize<MetadataDatabase>(jr);
            }
        }




        #endregion
    }
}
