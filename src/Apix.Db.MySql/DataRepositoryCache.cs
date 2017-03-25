using System;
using System.Linq;
using System.Reflection;

namespace Apix.Db.Mysql
{
    internal static class DataRepositoryCache
    {
        private static readonly PropertyCache GlobalPropertiesCache = new PropertyCache();

        public static PropertyInfo[] GetOrAdd(Type entityType)
        {
            var properties = GlobalPropertiesCache.GetOrAdd(entityType.TypeHandle, key =>
                                                                    (from p in entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                                                     let attr = p.GetCustomAttribute(typeof(NotDatabaseFieldAttribute))
                                                                     let attrSetting = p.GetCustomAttribute(typeof(DatabaseFieldAttribute))
                                                                     where attr == null 
                                                                           && !( attrSetting != null && !((DatabaseFieldAttribute)attrSetting).Ignore) 
                                                                           && p.GetSetMethod(true) != null && p.GetGetMethod(true) != null
                                                                     select p).ToDictionary(p => p.Name));
            return properties.Values.ToArray();
        }
    }
}