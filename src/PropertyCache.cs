using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;

namespace Apix.Db.Mysql
{
    internal class PropertyCache : 
        ConcurrentDictionary<RuntimeTypeHandle, Dictionary<string, PropertyInfo>>
    {
    }
}