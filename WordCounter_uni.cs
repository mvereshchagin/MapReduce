using System;
using System.Linq;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;
using System.Threading.Tasks;

namespace MyBigData
{
    public class WordCounter : MapReduceMaster<string, string, string, int, int> //string, int, string, int, int
    {
        protected override IEnumerable<KeyValuePair<string, int>> DoMap(string key, string value)
        {
            var result = new List<string, int>();
            foreach (var word in value.Split(" "))
                list.Add(new KeyValuePair<string, int>(word, 1));
            return list;
        }

        protected override IEnumerable<int> DoReduce(string key, IEnumerable<int> value)
        {
            int sum = 0;
            foreach (var value in value)
            {
                sum += value;
            }
            return new int[1] { sum };
        }

    }
}
