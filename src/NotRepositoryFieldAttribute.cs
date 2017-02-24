using System;

namespace Apix.Db.Mysql
{
    /// <summary>
    /// Mark class properties as not used for database queries
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class NotRepositoryFieldAttribute : Attribute
    {
    }

    /// <summary>
    /// Mark class properties as not used for <see cref="DeltaDataData{T}"/> operations
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class NotPatchableAttribute : Attribute
    {
    }

    /// <summary>
    /// Mark class properties as not used for database queries
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class NotDatabaseFieldAttribute : Attribute
    {
    }
}