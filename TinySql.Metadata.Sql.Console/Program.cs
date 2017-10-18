using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using TinySql.Serialization;

namespace TinySql.Metadata.Sql.CommandLine
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                WriteHelp();
                Environment.Exit(0);
            }
            ParseArgs();
            if (Command == Commands.Unknown)
            {
                WriteHelp();
                Environment.Exit(1);
            }
            if (string.IsNullOrEmpty(ConnectionString))
            {
                WriteHelp();
                Environment.Exit(1);
            }
            if (string.IsNullOrEmpty(OutputFile))
            {
                OutputFile = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "TinySql", Path.GetRandomFileName() + ".json");
            }

            if (Command == Commands.CreateMetadata)
            {
                Environment.Exit(CreateMetadata());
            }
            else if (Command == Commands.UpdateMetadata)
            {
                Environment.Exit(CreateMetadata(true));
            }
            else if (Command == Commands.ExportProperties)
            {
                Environment.Exit(ExportProperties());
            }
        }

        private static int ExportProperties()
        {
            ConsoleColor c = Console.ForegroundColor;
            try
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Connecting to " + ConnectionString);
                SqlMetadataDatabase db = SqlMetadataDatabase.FromConnection(ConnectionString, true, File.Exists(OutputFile) ? OutputFile : null);
                db.MetadataUpdateEvent += db_MetadataUpdateEvent;
                DatabaseExtendedProperties props = db.ExportExtendedProperties();
                SerializationExtensions.ToFile<DatabaseExtendedProperties>(props, OutputFile, true);
                Console.WriteLine("Properties saved to {0}", OutputFile);
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                return 1;
            }
            finally
            {
                Console.ForegroundColor = c;
            }
            return 0;
        }

        private static int CreateMetadata(bool Update = false)
        {
            ConsoleColor c = Console.ForegroundColor;
            try
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Connecting to " + ConnectionString);
                SqlMetadataDatabase db = SqlMetadataDatabase.FromConnection(ConnectionString, Update, File.Exists(OutputFile) ? OutputFile : null);
                Console.WriteLine("{0} metadata for {1} tables and views...",Update ? "Updating" : "Building", Tables == null ? "all" : Tables.Length.ToString());
                if (Update)
                {
                    db.FileName = OutputFile;
                }
                db.MetadataUpdateEvent += db_MetadataUpdateEvent;
                if (Update && !File.Exists(OutputFile))
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("'Update' was specified but the file {0} does not exist. 'Create' will be used in stead", OutputFile);
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Update = false;
                    db.FileName = null;
                }
                MetadataDatabase mdb = db.BuildMetadata(true, Tables,Update);
                Console.WriteLine("Done. Metadata contains {0} tables", mdb.Tables.Count);
                Console.WriteLine("Saving metadata in the file {0}", OutputFile);
                ClassCreationOptions.MetadataEvent = new MetadataUpdateDelegate(db_MetadataUpdateEvent);
                ClassGenerator.CreateClasses(mdb);
                mdb.ToFile(OutputFile);
                return 0;
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                return 1;
            }
            finally
            {
                Console.ForegroundColor = c;
                if (PromptUser)
                {
                    Console.Write("Finished. Press any key to exit");
                    Console.ReadKey(true);
                }
                
            }


        }

        static void db_MetadataUpdateEvent(int PercentDone, string Message, DateTime timestamp)
        {
            Console.WriteLine("{0}%: {1} at {2}", PercentDone,Message, timestamp);
        }


        private static void WriteHelp()
        {
            Console.WriteLine("Command line parameters for creating and updating metadata and classes:");
            Console.WriteLine("(required) [update|create|Exportprop|Importprop] Will either create or update metadata. if 'update' is specified, you must also specify the output parameter to point to an existing metadata file to be updated");
            Console.WriteLine("(required) [connection|con]:\"<sql connection string>\": The connection string to use");
            Console.WriteLine("(optional) [output|out]:\"<path to output file>\": Path to the file where the metadata is json serialized. Specify an existing file to update the file with modified metadata from the database");
            Console.WriteLine("(optional) [tables]:<table1,table2...table n>: Comma separated list of tables to generate metadata for");
            Console.WriteLine("(optional) [wait] will prompt the user to press a key before closing the Console");
            Console.WriteLine("----------");
            Console.WriteLine("(optional) [class] Will generate one class file per table");
            Console.WriteLine("(optional) [partial] Will generate classes with the 'partial' keyword");
            Console.WriteLine("(optional) [using]:<namespace1,namespace2...namespace n> the using statements to create at the beginning of each class file");
            Console.WriteLine("(optional) [namespace|ns]:<namespace> the name space to create the classes under");
            Console.WriteLine("(optional) [fields] will generate each column as a public field. If not specified the columns will be created as public properties");
            Console.WriteLine("(optional) [attributes|attrib] will decorate the properties/fields with Foreign key/Primary key attributes");
            Console.WriteLine("(optional) [folder]:\"<Path to the output folder for classes>\"");


        }



        private static Dictionary<string, string> commands = new Dictionary<string, string>();
        private static string ConnectionString = "";
        private static string OutputFile = "";
        private static string[] Tables;
        private static bool PromptUser = false;
        private enum Commands
        {
            Unknown,
            CreateMetadata,
            UpdateMetadata,
            ExportProperties,
            ImportProperties
        }
        private static Commands Command = Commands.Unknown;
        private static bool CreateClasses = false;
        private static string Clean(string s)
        {
            return s.TrimStart('"').TrimEnd('"');
        }

        private static string[] SplitCommand(string cmd)
        {
            int i = cmd.IndexOf(':');
            if (i < 0)
            {
                return new string[] { cmd };
            }
            else
            {
                return new string[] { cmd.Substring(0, i), cmd.Substring(i + 1) };
            }

        }
        private static void ParseArgs()
        {
            string[] s = System.Environment.GetCommandLineArgs();
            for (int i = 1; i < s.Length; i++)
            {
                string[] cmd = SplitCommand(s[i]);
                if (cmd.Length == 2)
                {
                    if (cmd[0].Equals("connection", StringComparison.OrdinalIgnoreCase) || cmd[0].Equals("con", StringComparison.OrdinalIgnoreCase))
                    {
                        ConnectionString = Clean(cmd[1]);
                    }
                    else if (cmd[0].Equals("output", StringComparison.OrdinalIgnoreCase) || cmd[0].Equals("out", StringComparison.OrdinalIgnoreCase))
                    {
                        OutputFile = Clean(cmd[1]);
                    }
                    else if (cmd[0].Equals("tables", StringComparison.OrdinalIgnoreCase))
                    {
                        Tables = Clean(cmd[1]).Split(',');
                    }
                    else if (cmd[0].Equals("using", StringComparison.OrdinalIgnoreCase))
                    {
                        ClassCreationOptions.Usings = Clean(cmd[1]).Split(',').ToList();
                    }
                    else if (cmd[0].Equals("namespace", StringComparison.OrdinalIgnoreCase) || cmd[0].Equals("ns", StringComparison.OrdinalIgnoreCase))
                    {
                        ClassCreationOptions.Namespace = Clean(cmd[1]);
                    }
                    else if (cmd[0].Equals("folder", StringComparison.OrdinalIgnoreCase))
                    {
                        ClassCreationOptions.OutputPath = Clean(cmd[1]);
                    }
                }
                else
                {
                    if (cmd[0].Equals("create", StringComparison.OrdinalIgnoreCase))
                    {
                        Command = Commands.CreateMetadata;
                    }
                    else if (cmd[0].Equals("update", StringComparison.OrdinalIgnoreCase))
                    {
                        Command = Commands.UpdateMetadata;
                    }
                    else if (cmd[0].Equals("exportprop", StringComparison.OrdinalIgnoreCase))
                    {
                        Command = Commands.ExportProperties;
                    }
                    else if (cmd[0].Equals("importprop", StringComparison.OrdinalIgnoreCase))
                    {
                        Command = Commands.ImportProperties;
                    }
                    else if (cmd[0].Equals("wait", StringComparison.OrdinalIgnoreCase))
                    {
                        PromptUser = true;
                    }
                    else if (cmd[0].Equals("class", StringComparison.OrdinalIgnoreCase))
                    {
                        CreateClasses = true;
                    }
                    else if (cmd[0].Equals("partial", StringComparison.OrdinalIgnoreCase))
                    {
                        ClassCreationOptions.PartialClass = true;
                    }
                    else if (cmd[0].Equals("fields", StringComparison.OrdinalIgnoreCase))
                    {
                        ClassCreationOptions.ColumnAsProperty = false;
                    }
                    else if (cmd[0].Equals("attributes", StringComparison.OrdinalIgnoreCase) || cmd[0].Equals("attrib", StringComparison.OrdinalIgnoreCase))
                    {
                        ClassCreationOptions.DecorateColumnAttributes = true;
                    }
                }
            }
        }


    }
}
