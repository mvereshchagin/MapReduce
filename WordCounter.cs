using System;
using System.Linq;
using System.Collections.Generic;

namespace MapReduce
{
    public class WordCounter : MapReduceMaster3<string, string, string, int, int>
    {
        protected override IEnumerable<KeyValuePair<string, int>> DoMap(string key, string value)
        {
            List<KeyValuePair<string, int>> result = new List<KeyValuePair<string, int>>();
            foreach (var word in value.Split(' '))
                result.Add(new KeyValuePair<string, int>(word, 1));
            return result;
        }

        protected override IEnumerable<int> DoReduce(string key, IEnumerable<int> values)
        {
            int sum = 0;
            foreach (int value in values)
            {
                sum += value;
            }
            return new int[1] { sum };
        }
    }
}
