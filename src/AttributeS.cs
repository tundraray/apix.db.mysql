using System;

namespace Apix.Db.Mysql
{
    /// <summary>
    /// Mark class properties as not used for database queries
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class NotDatabaseFieldAttribute : Attribute
    {
    }

    /// <summary>
    /// Mark property with specific settings
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class DatabaseFieldAttribute : Attribute
    {
        public bool Ignore;
        public string Name { get; set; }
        public bool AutoIncrement { get; set; }
        public bool Identity { get; set; }

        public DatabaseFieldAttribute(string name, bool autoIncrement = false, bool ignore = false, bool identity = false)
        {
            Ignore = ignore;
            Name = name;
            Identity = identity;
            AutoIncrement = autoIncrement;
        }
        
    }

    [AttributeUsage(AttributeTargets.Class)]
    public sealed class DatabaseTableAttribute : Attribute
    {

        public string TableName { get; set; }

        public DatabaseTableAttribute(string tableName)
        {
            TableName = tableName;
        }

    }

    ///<summary>
    /// Attribute used to decorate enumerations with reader friendly names
    ///</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class EnumTextValueAttribute : Attribute
    {
        ///<summary>
        /// Returns the text representation of the value
        ///</summary>
        public string Text { get; }

        ///<summary>
        /// Allows the creation of a friendly text representation of the enumeration.
        ///</summary>
        /// <param name="text">The reader friendly text to decorate the enum.</param>
        public EnumTextValueAttribute(string text)
        {
            Text = text;
        }
    }
}