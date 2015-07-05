﻿/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System.Collections.Generic;
using System.Diagnostics;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.InProcess.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IMRU.InProcess
{
    /// <summary>
    /// Implements the IMRU client API for in-process execution
    /// </summary>
    /// <remarks>
    /// This client assumes that all given Configurations can be merged in a conflict-free way.
    /// </remarks>
    /// <typeparam name="TMapInput">The type of the side information provided to the Map function</typeparam>
    /// <typeparam name="TMapOutput">The return type of the Map function</typeparam>
    /// <typeparam name="TResult">The return type of the computation.</typeparam>
    public class InProcessIMRUClient<TMapInput, TMapOutput, TResult> : IIMRUClient<TMapInput, TMapOutput, TResult>
    {
        private readonly int _numberOfMappers;

        /// <summary>
        /// Use Tang to instantiate this.
        /// </summary>
        /// <param name="numberOfMappers">The number of mappers to instantiate</param>
        [Inject]
        private InProcessIMRUClient([Parameter(typeof(NumberOfMappers))] int numberOfMappers)
        {
            Debug.Assert(numberOfMappers > 0);
            _numberOfMappers = numberOfMappers;
        }

        public IEnumerable<TResult> Submit(IMRUJobDefinition jobDefinition)
        {
            var injector = TangFactory.GetTang().NewInjector(jobDefinition.Configuration);

            injector.BindVolatileInstance(GenericType<MapFunctions<TMapInput, TMapOutput>>.Class,
                MakeMapFunctions(injector));

            var runner = injector.GetInstance<IMRURunner<TMapInput, TMapOutput, TResult>>();
            return runner.Run();
        }

        private MapFunctions<TMapInput, TMapOutput> MakeMapFunctions(IInjector injector)
        {
            ISet<IMapFunction<TMapInput, TMapOutput>> mappers = new HashSet<IMapFunction<TMapInput, TMapOutput>>();
            for (var i = 0; i < _numberOfMappers; ++i)
            {
                mappers.Add(injector.ForkInjector().GetInstance<IMapFunction<TMapInput, TMapOutput>>());
            }
            return new MapFunctions<TMapInput, TMapOutput>(mappers);
        }
    }
}