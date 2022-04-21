using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.Remoting;


namespace MapReduce
{
    public class NaiveMapReduceMaster<MapKey, MapValue, ReduceKey, ReduceValue, ReduceResult>
    {

        public delegate IEnumerable<KeyValuePair<ReduceKey, ReduceValue>> MapFunction(MapKey key, MapValue value);
        public delegate IEnumerable<ReduceResult> ReduceFunction(ReduceKey key, IEnumerable<ReduceValue> values);
        private MapFunction map;
        private ReduceFunction reduce;

        public NaiveMapReduceMaster(MapFunction mapFunction, ReduceFunction reduceFunction)
        {
            this.map = mapFunction;
            this.reduce = reduceFunction;
        }

        private IEnumerable<KeyValuePair<ReduceKey, ReduceValue>> Map(IEnumerable<KeyValuePair<MapKey, MapValue>> input)
        {
            var q = from pair in input
                    from mapped in map(pair.Key, pair.Value)
                    select mapped;

            return q;
        }

        private IEnumerable<KeyValuePair<ReduceKey, ReduceResult>> Reduce(IEnumerable<KeyValuePair<ReduceKey, ReduceValue>> intermediateValues)
        {
            // First, group intermediate values by key 
            var groups = from pair in intermediateValues
                         group pair.Value by pair.Key into g
                         select g;

            // Reduce on each group 
            var reduced = from g in groups
                          let k2 = g.Key
                          from reducedValue in reduce(k2, g)
                          select new KeyValuePair<ReduceKey, ReduceResult>(k2, reducedValue);

            return reduced;
        }

        /// <summary>
        /// Execute Map reduce algorithm
        /// </summary>
        /// <returns>The result of Map Reduce operation</returns>
        /// <param name="input">Data</param>
        public IEnumerable<KeyValuePair<ReduceKey, ReduceResult>> Execute(IEnumerable<KeyValuePair<MapKey, MapValue>> input)
        {
            return Reduce(Map(input));
        }
    }
}
