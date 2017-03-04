using System.Reflection;
using Apix.Extensions;

namespace Apix.Db.Mysql
{
    internal  static class PropertyHelper
    {

        public static string GetTableName(this TypeInfo type)
        {
            return type.GetCustomAttribute<DatabaseTableAttribute>()?.TableName ?? type.Name;
        }
       
        public static DatabaseFieldAttribute GetDatabaseFieldAttribute(this PropertyInfo property)
        {
            return property.GetCustomAttribute<DatabaseFieldAttribute>();
        }

        public static DatabaseFieldAttribute GetDatabaseFieldAttribute(this MemberInfo property)
        {
            return property.GetCustomAttribute<DatabaseFieldAttribute>();
        }

        public static string GetDatabaseFieldName(this PropertyInfo property)
        {
            return property.GetDatabaseFieldAttribute() != null
                   && property.GetDatabaseFieldAttribute().Name.IsNotNullOrEmpty()
                ? property.GetDatabaseFieldAttribute().Name
                : property.Name;
        }

        public static string GetDatabaseFieldName(this MemberInfo property)
        {
            return property.GetDatabaseFieldAttribute() != null
                   && property.GetDatabaseFieldAttribute().Name.IsNotNullOrEmpty()
                ? property.GetDatabaseFieldAttribute().Name
                : property.Name;
        }

        public static bool IsDatabaseIdentity(this PropertyInfo property)
        {
            return property.GetDatabaseFieldAttribute() != null && property.GetDatabaseFieldAttribute().Identity;
        }

        public static bool IsDatabaseAutoIncrement(this PropertyInfo property)
        {
            return property.GetDatabaseFieldAttribute() != null && property.GetDatabaseFieldAttribute().AutoIncrement;
        }

        public static bool IsNotDatabaseField(this PropertyInfo property)
        {
            return property.GetCustomAttribute(typeof(NotDatabaseFieldAttribute)) != null
                   || (property.GetDatabaseFieldAttribute() != null && property.GetDatabaseFieldAttribute().Ignore);
        }
    }
}
