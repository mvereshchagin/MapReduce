using System;
using System.Collections.Generic;
using System.Linq;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace MapReduce
{
    public abstract class MapReduceMaster3<MapKey, MapValue, ReduceKey, ReduceValue, ReduceResult>
    {
        public int TopCount { get; set; }
        public int DegreeOfParallelism { get; set; }

        protected abstract IEnumerable<KeyValuePair<ReduceKey, ReduceValue>> DoMap(MapKey key, MapValue value);

        protected abstract IEnumerable<ReduceResult> DoReduce(ReduceKey key, IEnumerable<ReduceValue> values);

        private IEnumerable<KeyValuePair<ReduceKey, ReduceValue>> Map(IEnumerable<KeyValuePair<MapKey, MapValue>> input)
        {
            var q = from pair in input//.AsParallel().WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                    from mapped in this.DoMap(pair.Key, pair.Value)
                    select mapped;

            return q;
        }


        private IEnumerable<KeyValuePair<ReduceKey, ReduceResult>> Reduce(IEnumerable<KeyValuePair<ReduceKey, ReduceValue>> intermediateValues)
        {
            // First, group intermediate values by key 
            var groups = from pair in intermediateValues//.AsParallel().WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                         group pair.Value by pair.Key into g
                         select g;
            // Reduce on each group 
            var reduced = (from g in groups//.AsParallel().WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                           let k2 = g.Key
                           from reducedValue in this.DoReduce(k2, g)
                           select new KeyValuePair<ReduceKey, ReduceResult>(k2, reducedValue)).OrderByDescending(x => x.Value).Take(10);

            return reduced;
        }

        public IEnumerable<KeyValuePair<ReduceKey, ReduceResult>> Execute(IEnumerable<KeyValuePair<MapKey, MapValue>> input)
        {
            return Reduce(Map(input));
        }
    }
}
