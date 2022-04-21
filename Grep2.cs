using System;
using System.Collections.Generic;
using System.IO;

namespace MapReduce
{
    public class Grep2 : MapReduceMaster2<string, Stream, string, int, int>
    {
        private string searchPattern;
        private bool caseInsensitive;

        public Grep2(string searchPattern, bool caseInsensitive = false)
        {
            this.searchPattern = searchPattern;
            this.caseInsensitive = caseInsensitive;
        }

        protected override IEnumerable<KeyValuePair<string, int>> DoMap(string key, Stream value)
        {
            List<KeyValuePair<string, int>> result = new List<KeyValuePair<string, int>>();

            string str = null;
            using (StreamReader reader = new StreamReader(value))
            {
                str = reader.ReadToEnd();
            }

            string data = (this.caseInsensitive ? str.ToLower() : str);
            string pattern = (this.caseInsensitive ? this.searchPattern.ToLower() : this.searchPattern);

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

        public static Stream GenerateStreamFromString(string s)
        {
            var stream = new MemoryStream();
            var writer = new StreamWriter(stream);
            writer.Write(s);
            writer.Flush();
            stream.Position = 0;
            return stream;
        }
    }
}
