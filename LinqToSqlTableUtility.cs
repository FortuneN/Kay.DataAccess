using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.Data.Linq.Mapping;
using System.Xml.Linq;
using System.IO;
using System.Data.Linq;

namespace Kay.DataAccess
{
	/// <summary>
	/// TODO: Update summary.
	/// </summary>
	public class LinqToSqlTableUtility
    {
        //Fields

        private ITable itable;
        private DataContext dataContext;
        private Type itemType;
        private TableAttribute tableAttribute;
        private PropertyInfo primaryPropertyInfo;

        //Properties

        public string TableName { get { return tableAttribute.Name; } }

        public string PrimaryKeyName { get { return primaryPropertyInfo.Name; } }

        //Contructors

        public LinqToSqlTableUtility(Type itemType, TableAttribute tableAttribute, PropertyInfo primaryPropertyInfo) : this(null, itemType, tableAttribute, primaryPropertyInfo) { }
        public LinqToSqlTableUtility(DataContext dataContext, Type itemType, TableAttribute tableAttribute, PropertyInfo primaryPropertyInfo)
        {
            this.dataContext = dataContext;
            if (dataContext != null) this.itable = dataContext.GetTable(itemType);
            this.itemType = itemType;
            this.tableAttribute = tableAttribute;
            this.primaryPropertyInfo = primaryPropertyInfo;
        }

        //Methods

        public object GetPrimaryKeyValue(object item)
        {
            return primaryPropertyInfo.GetValue(item, null);
        }

        public void SetPrimaryKeyValue(object item, object value)
        {
            primaryPropertyInfo.SetValue(item, value, null);
        }

        public XElement GetRecordSnapshot(object item)
        {
            Type type = item.GetType();
            StringBuilder sbXml = new StringBuilder();

            sbXml.AppendFormat("<{0}>", type.Name);
            foreach (PropertyInfo propety in type.GetProperties())
            {
                string propertyTypeName = propety.PropertyType.Name;
                if (propertyTypeName.Contains("EntitySet") || propertyTypeName.Contains("EntityRef")) continue;
                sbXml.AppendFormat("<{0}><![CDATA[{1}]]></{0}>", propety.Name, propety.GetValue(item, null));
            }
            sbXml.AppendFormat("</{0}>", type.Name);

            XDocument document = XDocument.Load(new StringReader(sbXml.ToString()));
            return document.Root;
        }

        //Static

        private static Dictionary<string, LinqToSqlTableUtility> cache = new Dictionary<string, LinqToSqlTableUtility>();

        public static LinqToSqlTableUtility GetByType(Type itemType)
        {
            LinqToSqlTableUtility result = null;

            string key = itemType.Name;
            if (!cache.ContainsKey(key) || (result = cache[key]) == null)
            {
                TableAttribute table = (TableAttribute)itemType.GetCustomAttributes(typeof(TableAttribute), false)[0];

                foreach (PropertyInfo property in itemType.GetProperties())
                {
                    foreach (ColumnAttribute column in property.GetCustomAttributes(typeof(ColumnAttribute), false))
                    {
                        if (column.IsPrimaryKey) result = new LinqToSqlTableUtility(itemType, table, property);
                        if (result != null) break;
                    }
                    if (result != null) break;
                }

                cache[key] = result;
            }

            return result;
        }
    }
}
