using System;
using System.Collections.Generic;
using System.Linq;
using System.Collections.Concurrent;

namespace MapReduce
{
    public abstract class MapReduceMaster2<MapKey, MapValue, ReduceKey, ReduceValue, ReduceResult>
    {
        protected abstract IEnumerable<KeyValuePair<ReduceKey, ReduceValue>> DoMap(MapKey key, MapValue value);

        protected abstract IEnumerable<ReduceResult> DoReduce(ReduceKey key, IEnumerable<ReduceValue> values);
        //private IEnumerable<KeyValuePair<ReduceKey, ReduceValue>> Map(IEnumerable<KeyValuePair<MapKey, MapValue>> input)
        //{
        //    var q = from pair in input
        //            from mapped in this.DoMap(pair.Key, pair.Value)
        //            select mapped;

        //    return q;
        //}

        //private IEnumerable<KeyValuePair<ReduceKey, ReduceValue>> Map(IEnumerable<KeyValuePair<MapKey, MapValue>> input)
        //{
        //    foreach(var pair in input)
        //        foreach (var mapped in this.DoMap(pair.Key, pair.Value))
        //            yield return new KeyValuePair<ReduceKey, ReduceValue>(mapped.Key, mapped.Value);
        //}

        private IEnumerable<KeyValuePair<ReduceKey, ReduceValue>> Map(IEnumerable<KeyValuePair<MapKey, MapValue>> input)
        {
            //List<KeyValuePair<ReduceKey, ReduceValue>> result = new List<KeyValuePair<ReduceKey, ReduceValue>>();
            var result = new ConcurrentBag<KeyValuePair<ReduceKey, ReduceValue>>();
            foreach (var pair in input)
                foreach (var mapped in this.DoMap(pair.Key, pair.Value))
                    result.Add( new KeyValuePair<ReduceKey, ReduceValue>(mapped.Key, mapped.Value));
            return result;
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
                          from reducedValue in this.DoReduce(k2, g)
                          select new KeyValuePair<ReduceKey, ReduceResult>(k2, reducedValue);

            return reduced;
        }

        public IEnumerable<KeyValuePair<ReduceKey, ReduceResult>> Execute(IEnumerable<KeyValuePair<MapKey, MapValue>> input)
        {
            return Reduce(Map(input));
        }
    }
}
