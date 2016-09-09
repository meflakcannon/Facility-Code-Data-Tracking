using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Data;
using System.IO;

namespace FacilityCodeRejects
{
    public static class Program
    {
        static void Main(string[] args)
        {
            //Get Desktop of Current User to dump CSV with Timestamp there.
            string desktop = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
            string path = desktop + "\\SASDATA\\", output;
            string startdate = "2016-09-08", enddate = "2016-09-09";
            string SASDatabase = "ccusatprd0", authuser = "reportsPrdS";
            string Connectiondb, Connectionuser;

            if (!Directory.Exists(path)) Directory.CreateDirectory(path);

            Console.WriteLine("Please enter the start date. Gives you everything from this date (FORMAT: YYYY-MM-DD -- 2016-09-08)");
            startdate = Console.ReadLine();
            Console.WriteLine("Please enter the end date. Gives you everything up until date, nothing from that date (FORMAT: YYYY-MM-DD -- 2016-09-10)");
            enddate = Console.ReadLine();

            Console.WriteLine("Setting up connection to databases");
            Console.WriteLine("Setting up SQL query for facility code rejects");

            SqlCommand Query = new SqlCommand();
            SqlDataReader reader = null; // A Container to read the data coming back

            Query.CommandText = "SELECT DISTINCT CAST(XmlMessage as xml).value('(/LogMessage//CHUID//Card/node())[1]', 'nvarchar(max)') as CardNumber, CAST(XmlMessage as xml).value('(/LogMessage//RejectCode/node())[1]', 'nvarchar(max)') as RejectCode, CAST(XmlMessage as xml).value('(/LogMessage//SecondaryObjectName/node())[1]', 'nvarchar(max)') as Door FROM SWHSystemJournal.dbo.JournalLog WHERE CAST(XmlMessage as xml).value('(/LogMessage//RejectCode/node())[1]', 'nvarchar(max)') = 'FacilityCode' AND MessageUTC >= '" + startdate + "' AND MessageUTC <= '" + enddate + "' AND CAST(XmlMessage as xml).value('(/LogMessage//CHUID//Card/node())[1]', 'nvarchar(max)') IS NOT NULL ORDER BY Door asc";
            Query.CommandType = CommandType.Text;
            // SAS 4 takes it's sweet ass time, requiring a longer timeout.
            Query.CommandTimeout = 1000;

            // Loops through SAS 1-6 (6 servers because thats all our org has) using the count of the foor loop to dictate Server and user (password is static). Read only account.
            for (int i = 1; i <= 6; i++)
            {
                Console.WriteLine("Logging in to SAS" + i);

                output = path + "\\SAS" + i + "FacilityCodeRejects.csv";

                Connectiondb = SASDatabase + i;
                Connectionuser = authuser + i;

                Query.Connection = sas;
                sas.Open();
                reader = Query.ExecuteReader();

                ToCsv(reader, output, true);
                sas.Close();
                Console.WriteLine("Closing connection to SAS" + i);
            }

            output = desktop + "\\FacilityCodeRejects-" + startdate + "-" + enddate + ".csv";
            
            var allCsv = Directory.EnumerateFiles(path);
            string[] header = { File.ReadLines(allCsv.First()).First(l => !string.IsNullOrWhiteSpace(l))};
            var mergedData = allCsv
                .SelectMany(csv => File.ReadLines(csv)
                    .SkipWhile(l => string.IsNullOrWhiteSpace(l)).Skip(1)); // skip header of each file
            File.WriteAllLines(output, header.Concat(mergedData));

            // Deletes the temp files leaving user with just the one file
            try
            {
                Directory.Delete(path, true);
            }
            catch (IOException)
            {
                Console.WriteLine("Error Deleting Temporary Data! Manual Cleanup may be needed");
            }
        }

        public static void ToCsv(this IDataReader dataReader, string fileName, bool includeHeaderAsFirstRow)
        {

            const string Separator = ",";

            StreamWriter streamWriter = new StreamWriter(fileName);

            StringBuilder sb = null;

            if (includeHeaderAsFirstRow)
            {
                sb = new StringBuilder();
                for (int index = 0; index < dataReader.FieldCount; index++)
                {
                    if (dataReader.GetName(index) != null)
                        sb.Append(dataReader.GetName(index));

                    if (index < dataReader.FieldCount - 1)
                        sb.Append(Separator);
                }
                streamWriter.WriteLine(sb.ToString());
            }

            while (dataReader.Read())
            {
                sb = new StringBuilder();
                for (int index = 0; index < dataReader.FieldCount - 1; index++)
                {
                    if (!dataReader.IsDBNull(index))
                    {
                        string value = dataReader.GetValue(index).ToString();
                        if (dataReader.GetFieldType(index) == typeof(String))
                        {
                            if (value.IndexOf("\"") >= 0)
                                value = value.Replace("\"", "\"\"");

                            if (value.IndexOf(Separator) >= 0)
                                value = "\"" + value + "\"";
                        }
                        sb.Append(value);
                    }

                    if (index < dataReader.FieldCount - 1)
                        sb.Append(Separator);
                }

                if (!dataReader.IsDBNull(dataReader.FieldCount - 1))
                    sb.Append(dataReader.GetValue(dataReader.FieldCount - 1).ToString().Replace(Separator, " "));

                streamWriter.WriteLine(sb.ToString());
            }
            dataReader.Close();
            streamWriter.Close();
        }
    }
}
