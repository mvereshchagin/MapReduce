using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;
using System.Net;
using System.Text;

namespace MapReduce
{
    class MainClass
    {
        //private static IDictionary<String, String> inputData = new Dictionary<string, String>() 
        //{ 
        //    { "doc1.txt", "test vvv test vsays dfddfds test vvv dsfdfdsfds" },
        //    { "doc2.txt", "test vasya test jjj toop" },
        //    { "doc3.txt", "ghgsadsa sadsas test vvv dsfdfdsfds" },
        //    { "doc4.txt", "ghgsadsa test test vvv dsfdfdsfds vvv rgre tfhfchgf" }
        //};

        private static string dirPath = "/mnt/data/Misha/Projects/csharp/Parallel/Data/";


        public static void Main(string[] args)
        {
            //NaiveMapReduceMaster<string, string, string, int, int> master = new NaiveMapReduceMaster<string, string, string, int, int>(MapFromMem, Reduce);
            //var result = master.Execute(inputData).ToDictionary(key => key.Key, v => v.Value);

            Dictionary<String, FileInfo> inputData = new Dictionary<string, FileInfo>();
            DirectoryInfo directoryInfo = new DirectoryInfo(dirPath);
            foreach(var file in directoryInfo.GetFiles())
            {
                inputData.Add(file.Name, file);
            }


            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            WordCounter2 wordCounter = new WordCounter2();
            var result = wordCounter.Execute(inputData);//.ToDictionary(key => key.Key, v => v.Value);
            //foreach (var key in result.Keys)
            //{
            //    String strValue = String.Format("{0}: {1}", key, result[key]);
            //    Console.WriteLine(strValue);
            //}

            foreach (var item in result)
            { 
                String strValue = String.Format("{0}: {1}", item.Key, item.Value);
                Console.WriteLine(strValue);
            }

            stopWatch.Stop();

            Console.WriteLine("Time Elasped is: " + stopWatch.Elapsed.Milliseconds.ToString() + " milliseconds");

            //Grep grep = new Grep("test");
            //var grepResult = grep.Execute(inputData).ToDictionary(key => key.Key, v => v.Value);
            //foreach (var key in grepResult.Keys)
            //{
            //    String strValue = String.Format("{0}: {1}", key, grepResult[key]);
            //    Console.WriteLine(strValue);
            //}

            //var path = "/home/misha/scripts";

            //IDictionary<String, Stream> inputData2 = new Dictionary<string, Stream>();
            //foreach(var file in System.IO.Directory.GetFiles(path))
            //{
            //    FileStream stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            //    BufferedStream bs = new BufferedStream(stream);
            //    inputData2.Add(new KeyValuePair<string, Stream>(file, bs));
            //}

            //foreach(var key in inputData.Keys)
            //{
            //    inputData2.Add(new KeyValuePair<string, Stream>(key, Grep2.GenerateStreamFromString(inputData[key])));
            //}



            //Grep2 grep2 = new Grep2("test");
            //var grepResult = grep2.Execute(inputData2).ToDictionary(key => key.Key, v => v.Value);
            //foreach (var key in grepResult.Keys)
            //{
            //    String strValue = String.Format("{0}: {1}", key, grepResult[key]);
            //    Console.WriteLine(strValue);
            //}
        }

    }

}
