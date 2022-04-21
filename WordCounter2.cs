using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace MapReduce
{
    public class WordCounter2 : MapReduceMaster3<string, FileInfo, string, int, int>
    {

        protected override IEnumerable<KeyValuePair<string, int>> DoMap(string key, FileInfo value)
        {
            var words = (from line in File.ReadLines(value.FullName)//.AsParallel()
                         from word in line.Split(' ')
                         select new KeyValuePair<string, int>(word, 1)).AsEnumerable();

            Console.WriteLine(String.Format("key = {0}; words = {1}", key, words.Count()));

            return words;
        }

        protected override IEnumerable<int> DoReduce(string key, IEnumerable<int> values)
        {
            return new int[] { values.Sum() };
        }
    }
}
