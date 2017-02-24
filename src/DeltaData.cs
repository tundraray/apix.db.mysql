using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using Apix.Dynamic;

namespace Apix.Db.Mysql
{
    public class DeltaData<T> : SerializableDynamicObject
    {
        /// <summary>
        /// Base entity class properties cache
        /// </summary>
        protected static readonly PropertyCache PropertyCache = new PropertyCache();

        private Dictionary<string, PropertyInfo> _propertiesThatExist;
        /// <summary>
        /// Default constructor
        /// </summary>
        public DeltaData() : this(typeof(T)) { }
        internal DeltaData(Type entityType)
        {
            Initialize(entityType);
        }
        /// <summary>
        /// Base entity type
        /// </summary>
        [IgnoreDataMember]
        public Type EntityType { get; private set; }
        /// <summary>
        /// Get property/value dictionary
        /// </summary>
        /// <returns>Return property/value dictionary</returns>
        public IDictionary<string, object> GetData()
        {
            return DynamicProperties;
        }
        /// <summary>
        /// Get dynamic value
        /// </summary>
        /// <param name="name">Member name</param>
        /// <returns>Member value</returns>
        public override object GetValue(string name)
        {
            Ensure.Argument.NotNullOrEmpty(name, "name");
            Ensure.That<NotSupportedException>(_propertiesThatExist.ContainsKey(name),
                "The property " + name + " is not supported in class type " + EntityType + " or marked as NotPatchable.");
            var property = _propertiesThatExist[name];
            var value = base.GetValue(name);
            // Fix types after serialization
            if (value == null || value.GetType() == property.PropertyType) return value;
            value = ConvertTo(value, property.PropertyType);
            base.SetValue(name, value);
            return value;
        }
        /// <summary>
        /// Set dynamic member value
        /// </summary>
        /// <param name="name">Member name</param>
        /// <param name="value">Member value</param>
        /// <returns>Stored member value</returns>
        public override object SetValue(string name, object value)
        {
            Ensure.Argument.NotNullOrEmpty(name, "name");
            Ensure.That<NotSupportedException>(_propertiesThatExist.ContainsKey(name),
                "The property " + name + " is not supported in class type " + EntityType + " or marked as NotPatchable.");
            var property = _propertiesThatExist[name];
            if (value == null && !IsNullable(property.PropertyType))
                throw new NullReferenceException("Property " + name + " can not be null.");

            if (value != null && value.GetType() != property.PropertyType)
            {
                value = ConvertTo(value, property.PropertyType);
            }

            return base.SetValue(name, value);
        }

        private void Initialize(Type entityType)
        {
            Ensure.Argument.NotNull(entityType, "entityType");
            Ensure.That<InvalidOperationException>(typeof(T).IsAssignableFrom(entityType),
                string.Format("The entity type '{0}' is not assignable to the DeltaData type '{1}'.", entityType, typeof(T)));
            EntityType = entityType;
            _propertiesThatExist = InitializePropertiesThatExist();
        }

        [OnDeserializing]
        private void OnDeserializing(StreamingContext ctx)
        {
            Initialize(typeof(T));
        }
        /// <summary>
        /// Initialize and return base entity properties collection
        /// </summary>
        /// <remarks>
        /// Method remove all base methods properties which marked by <see cref="NotPatchableAttribute"/>.
        /// </remarks>
        /// <returns>Return name/<see cref="PropertyInfo"/> dictionary</returns>
        protected virtual Dictionary<string, PropertyInfo> InitializePropertiesThatExist()
        {
            return PropertyCache.GetOrAdd(EntityType.TypeHandle, (from p in EntityType.GetProperties()
                                                                   let npAttr = p.GetCustomAttribute(typeof(NotPatchableAttribute))
                                                                   where npAttr == null && p.GetSetMethod() != null && p.GetGetMethod() != null
                                                                   select p).ToDictionary(p => p.Name));
        }

        private static object ConvertTo(object obj, Type type)
        {
            if (IsNullable(type) && obj == null)
                return null;

            if (IsNullable(type) && Nullable.GetUnderlyingType(type) != null)
                type = Nullable.GetUnderlyingType(type);

            if (type == typeof(byte))
                return Convert.ToByte(obj);
            if (type == typeof(short))
                return Convert.ToInt16(obj);
            if (type == typeof(ushort))
                return Convert.ToUInt16(obj);
            if (type == typeof(int))
                return Convert.ToInt32(obj);
            if (type == typeof(uint))
                return Convert.ToUInt32(obj);
            if (type == typeof(long))
                return Convert.ToInt64(obj);
            if (type == typeof(ulong))
                return Convert.ToUInt64(obj);
            if (type == typeof(float))
                return Convert.ToSingle(obj);
            if (type == typeof(double))
                return Convert.ToDouble(obj);
            if (type == typeof(decimal))
                return Convert.ToDecimal(obj);
            if (type == typeof(bool))
                return Convert.ToBoolean(obj);
            if (type == typeof(string))
                return obj == null ? null : Convert.ToString(obj);
            if (type == typeof(char))
                return Convert.ToChar(obj);
            if (type == typeof(Guid))
                return new Guid(obj.ToString());
            if (type == typeof(DateTime))
                return ConvertDateTime(obj);
            if (type == typeof(DateTimeOffset))
            {
                if (obj is string)
                    return DateTimeOffset.Parse(obj.ToString());
                return (DateTimeOffset)obj;
            }
            return obj;
        }

        private static DateTime ConvertDateTime(object obj)
        {
            if (!(obj is string)) return Convert.ToDateTime(obj);
            var dt = DateTimeOffset.Parse(obj.ToString());
            return dt.DateTime;
        }

        private static bool IsNullable(Type type)
        {
            return !type.GetTypeInfo().IsValueType || Nullable.GetUnderlyingType(type) != null;
        }
    }

}