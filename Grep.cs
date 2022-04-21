using System;
using System.Collections.Generic;

namespace MapReduce
{
    public class Grep : MapReduceMaster2<string, string, string, int, int>
    {
        private string searchPattern;
        private bool caseInsensitive;

        public Grep(string searchPattern, bool caseInsensitive = false)
        {
            this.searchPattern = searchPattern;
            this.caseInsensitive = caseInsensitive;
        }

        protected override IEnumerable<KeyValuePair<string, int>> DoMap(string key, string value)
        {
            List<KeyValuePair<string, int>> result = new List<KeyValuePair<string, int>>();
            string data = ( this.caseInsensitive ? value.ToLower() : value );
            string pattern = ( this.caseInsensitive ? this.searchPattern.ToLower() : this.searchPattern );

            int i = 0;
            while ((i = data.IndexOf(pattern, i, StringComparison.CurrentCulture)) != -1)
            {
                i += pattern.Length;
                result.Add(new KeyValuePair<string, int>(key, 1));
            }
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
