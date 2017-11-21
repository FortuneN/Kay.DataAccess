using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.Linq;
using System.Data.Linq.Mapping;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Transactions;

namespace Kay.DataAccess
{
	public class LinqToSqlDao<TEntity, TDataContext, TConnectionStringReader> where TEntity : class, new() where TDataContext : DataContext, new() where TConnectionStringReader : IConnectionStringReader, new()
	{
		//Static

		private static IConnectionStringReader connectionStringReader = new TConnectionStringReader();
		private static Dictionary<Type, IEnumerable<PropertyInfo>> entityPrimaryKeyColumnProperties = new Dictionary<Type, IEnumerable<PropertyInfo>>();
		private static Dictionary<Type, string> entityTableName = new Dictionary<Type, string>();
		private static Dictionary<Type, string> entitySelectSql = new Dictionary<Type, string>();
		private static Dictionary<string, object> GetCompositePrimaryKey(TEntity entity)
		{
			FailIf(entity == null, "Parameter (entity) cannot be null");

			var primaryKeyNamesAndValues = new Dictionary<string, object>();

			foreach (var primaryKeyColumnProperty in entityPrimaryKeyColumnProperties[typeof(TEntity)])
			{
				var primaryKeyValue = primaryKeyColumnProperty.GetValue(entity, null);
				primaryKeyNamesAndValues.Add(primaryKeyColumnProperty.Name, primaryKeyValue);
			}

			FailIf(primaryKeyNamesAndValues.Count() == 0, "Class '" + typeof(TEntity).FullName + "' does not have a primary key defined. A primary key is required");

			return primaryKeyNamesAndValues;
		}

		static LinqToSqlDao()
		{
			foreach (var tableProperty in typeof(TDataContext).GetProperties().Where(x => x.ToString().Contains("System.Data.Linq.Table")))
			{
				var entityType = tableProperty.PropertyType.GetGenericArguments().First();

				//entityPrimaryKeyColumnProperties

				entityPrimaryKeyColumnProperties[entityType] = entityType.GetProperties().Where(p => p.GetCustomAttributes(typeof(ColumnAttribute), true).Any(a => ((ColumnAttribute)a).IsPrimaryKey));

				//entitySelectSql

				var typeWhereSqlList = new List<string>();
				foreach (var primaryKeyProperty in entityPrimaryKeyColumnProperties[entityType]) typeWhereSqlList.Add(primaryKeyProperty.Name + " = {" + typeWhereSqlList.Count + "}");
				entityTableName[entityType] = ((TableAttribute)entityType.GetCustomAttributes(typeof(TableAttribute), true).FirstOrDefault()).Name;
				entitySelectSql[entityType] = "SELECT * FROM " + entityTableName[entityType] + " WHERE " + string.Join(" AND ", typeWhereSqlList.ToArray());
			}
		}

		//Add

		public TEntity Add(TEntity entity)
		{
			FailIf(entity == null || entity == DBNull.Value, "Parameter (entity) cannot be null");

			using (var entityDataContext = NewDataContext())
			{
				//add

				var entityTable = entityDataContext.GetTable<TEntity>();
				entityTable.InsertOnSubmit(entity);

				//commit

				entityDataContext.SubmitChanges();

				//return

				return entity;
			}
		}

		public List<TEntity> AddAll(IEnumerable<TEntity> entities)
		{
			FailIf(entities == null || entities.Count() == 0, "Parameter (entities) cannot be null or empty");

			using (var transaction = new TransactionScope())
			{
				var result = new List<TEntity>();

				foreach (var entity in entities)
				{
					result.Add(Add(entity));
				}

				transaction.Complete();

				return result;
			}
		}

		//Update

		public TEntity Update(TEntity entity, IEnumerable<string> properties = null)
		{
			FailIf(entity == null || entity == DBNull.Value, "Parameter (entity) cannot be null");

			using (var entityDataContext = NewDataContext())
			{
				var compositePrimaryKey = GetCompositePrimaryKey(entity);
				var databaseEntity = entityDataContext.ExecuteQuery<TEntity>(entitySelectSql[typeof(TEntity)], compositePrimaryKey.Values.ToArray()).SingleOrDefault();

				//update (scalar values only)

				var entityDataContextNamespace = entityDataContext.GetType().Namespace;

				foreach (PropertyDescriptor entityPropertyDescriptor in TypeDescriptor.GetProperties(entity))
				{
					var entityPropertyDescriptorTypeFullName = entityPropertyDescriptor.PropertyType.FullName;

					if (entityPropertyDescriptor.PropertyType.Namespace == entityDataContextNamespace || entityPropertyDescriptorTypeFullName.Contains("System.Data.Linq.EntityRef") || entityPropertyDescriptorTypeFullName.Contains("System.Data.Linq.EntitySet") || compositePrimaryKey.Keys.Contains(entityPropertyDescriptor.Name))
					{
						continue;
					}

					if (properties != null && !properties.Contains(entityPropertyDescriptor.Name))
					{
						continue;
					}

					var entityPropertyValue = entityPropertyDescriptor.GetValue(entity);
					entityPropertyDescriptor.SetValue(databaseEntity, entityPropertyValue);
				}

				//commit

				entityDataContext.SubmitChanges();

				//return

				return databaseEntity;
			}
		}

		public List<TEntity> UpdateAll(IEnumerable<TEntity> entities)
		{
			FailIf(entities == null || entities.Count() == 0, "Parameter (entities) cannot be null or empty");

			using (var transaction = new TransactionScope())
			{
				var result = new List<TEntity>();

				foreach (var entity in entities)
				{
					result.Add(Update(entity));
				}

				transaction.Complete();

				return result;
			}
		}

		//Delete

		public TEntity DeleteByPrimaryKey(Dictionary<string, object> compositePrimaryKey)
		{
			FailIf(compositePrimaryKey == null || compositePrimaryKey.Count() == 0, "Parameter (compositePrimaryKey) cannot be null or empty");

			using (var entityDataContext = NewDataContext())
			{
				var persistedEntity = entityDataContext.ExecuteQuery<TEntity>(entitySelectSql[typeof(TEntity)], compositePrimaryKey.Values.ToArray()).SingleOrDefault();

				//delete and commit

				entityDataContext.GetTable<TEntity>().DeleteOnSubmit(persistedEntity);
				entityDataContext.SubmitChanges();

				//return

				return persistedEntity;
			}
		}

		public TEntity DeleteByPrimaryKey(object primaryKey)
		{
			FailIf(primaryKey == null || primaryKey == DBNull.Value, "Parameter (primaryKey) cannot be null");

			var primaryKeyColumnProperties = entityPrimaryKeyColumnProperties[typeof(TEntity)];
			var primaryKeyName = primaryKeyColumnProperties.Select(x => x.Name).SingleOrDefault();

			var compositePrimaryKey = new Dictionary<string, object>();
			compositePrimaryKey.Add(primaryKeyName, primaryKey);

			return DeleteByPrimaryKey(compositePrimaryKey);
		}

		public TEntity Delete(TEntity entity)
		{
			FailIf(entity == null || entity == DBNull.Value, "Parameter (entity) cannot be null");

			var compositePrimaryKey = GetCompositePrimaryKey(entity);
			return DeleteByPrimaryKey(compositePrimaryKey);
		}

		public IEnumerable<TEntity> DeleteAll(IEnumerable<TEntity> entities)
		{
			FailIf(entities == null, "Parameter (entities) cannot be null or empty");

			using (var transaction = new TransactionScope())
			{
				var result = new List<TEntity>();

				foreach (var entity in entities)
				{
					result.Add(Delete(entity));
				}

				transaction.Complete();

				return result;
			}
		}
        public IEnumerable<TEntity> DeleteWhere(Expression<Func<TEntity, bool>> where)
        {
            var entityDataContext = NewDataContext();
            var entityQueryable = GetQueryableWhere(where, null, entityDataContext);
            return DeleteAll(entityQueryable.ToList());
        }
        
		//GetByPrimaryKey

		public TEntity GetByPrimaryKey(Dictionary<string, object> compositePrimaryKey)
		{
			FailIf(compositePrimaryKey == null || compositePrimaryKey.Count() == 0, "Parameter (compositePrimaryKey) cannot be null or empty");

			using (var entityDataContext = NewDataContext())
			{
				return entityDataContext.ExecuteQuery<TEntity>(entitySelectSql[typeof(TEntity)], compositePrimaryKey.Values.ToArray()).SingleOrDefault();
			}
		}

		public TEntity GetByPrimaryKey(object primaryKey)
		{
			FailIf(primaryKey == null || primaryKey == DBNull.Value, "Parameter (primaryKey) cannot be null");

			var primaryKeyColumnProperties = entityPrimaryKeyColumnProperties[typeof(TEntity)];
			var primaryKeyName = primaryKeyColumnProperties.Select(x => x.Name).SingleOrDefault();

			var compositePrimaryKey = new Dictionary<string, object>();
			compositePrimaryKey.Add(primaryKeyName, primaryKey);

			return GetByPrimaryKey(compositePrimaryKey);
		}

		//Util

		public TDataContext NewDataContext()
		{
			var dataContext = new TDataContext();
			dataContext.Connection.ConnectionString = connectionStringReader.GetConnectionString();
			return dataContext;
		}
		public delegate IQueryable<TEntity> QueryDelegate(IQueryable<TEntity> query);
		public delegate LoadOptions LoadOptionsDelegate(LoadOptions loadOptions);
		
		private static IQueryable<TEntity> GetQueryableQuery(QueryDelegate query, LoadOptionsDelegate loadOptions, TDataContext entityDataContext)
		{
			var entityQueryable = entityDataContext.GetTable<TEntity>().AsQueryable<TEntity>();
			var entityLoadOptions = new LoadOptions();

			if (query != null)
			{
				entityQueryable = query(entityQueryable);
			}

			if (loadOptions != null)
			{
				entityDataContext.LoadOptions = loadOptions(entityLoadOptions).Options;
			}

			return entityQueryable;
		}
		private static IQueryable<TEntity> GetQueryableWhere(Expression<Func<TEntity, bool>> where, LoadOptionsDelegate loadOptions, TDataContext entityDataContext)
		{
			var entityQueryable = entityDataContext.GetTable<TEntity>().AsQueryable<TEntity>();
			var entityLoadOptions = new LoadOptions();

			if (where != null)
			{
				entityQueryable = entityQueryable.Where(where);
			}

			if (loadOptions != null)
			{
				entityDataContext.LoadOptions = loadOptions(entityLoadOptions).Options;
			}

			return entityQueryable;
		}

		public IQueryable<TEntity> Query(QueryDelegate query = null, LoadOptionsDelegate loadOptions = null)
		{
			var entityDataContext = NewDataContext();
			var entityQueryable = GetQueryableQuery(query, loadOptions, entityDataContext);
			return entityQueryable;
		}
		public IQueryable<TEntity> Where(Expression<Func<TEntity, bool>> where = null, LoadOptionsDelegate loadOptions = null)
		{
			var entityDataContext = NewDataContext();
			var entityQueryable = GetQueryableWhere(where, loadOptions, entityDataContext);
			return entityQueryable;
		}

		//Get

		public ResultSet<TEntity> Get()
		{
			return Get(null, null, null, null);
		}

		public ResultSet<TEntity> Get(int? pageSize, int? pageIndex)
		{
			return Get(null, null, pageSize, pageIndex);
		}

		public ResultSet<TEntity> Get(QueryDelegate query)
		{
			return Get(query, null, null, null);
		}

		public ResultSet<TEntity> Get(QueryDelegate query, LoadOptionsDelegate loadOptions)
		{
			return Get(query, loadOptions, null, null);
		}

		public ResultSet<TEntity> Get(QueryDelegate query, int? pageSize, int? pageIndex)
		{
			return Get(query, null, pageSize, pageIndex);
		}

		public ResultSet<TEntity> Get(QueryDelegate query, LoadOptionsDelegate loadOptions, int? pageSize, int? pageIndex)
		{
			using (var entityDataContext = NewDataContext())
			{
				var entityQueryable = GetQueryableQuery(query, loadOptions, entityDataContext);
				return new ResultSet<TEntity>(entityQueryable, pageSize, pageIndex);
			}
		}

		public ResultSet<TEntity> GetWhere()
		{
			return GetWhere(null, null, null, null);
		}

		public ResultSet<TEntity> GetWhere(int? pageSize, int? pageIndex)
		{
			return GetWhere(null, null, pageSize, pageIndex);
		}

		public ResultSet<TEntity> GetWhere(Expression<Func<TEntity, bool>> where)
		{
			return GetWhere(where, null, null, null);
		}

		public ResultSet<TEntity> GetWhere(Expression<Func<TEntity, bool>> where, LoadOptionsDelegate loadOptions)
		{
			return GetWhere(where, loadOptions, null, null);
		}

		public ResultSet<TEntity> GetWhere(Expression<Func<TEntity, bool>> where, int? pageSize, int? pageIndex)
		{
			return GetWhere(where, null, pageSize, pageIndex);
		}

		public ResultSet<TEntity> GetWhere(Expression<Func<TEntity, bool>> where, LoadOptionsDelegate loadOptions, int? pageSize, int? pageIndex)
		{
			using (var entityDataContext = NewDataContext())
			{
				var entityQueryable = GetQueryableWhere(where, loadOptions, entityDataContext);
				return new ResultSet<TEntity>(entityQueryable, pageSize, pageIndex);
			}
		}

		//SingleOrDefault

		public TEntity SingleOrDefault()
		{
			return SingleOrDefault(null, null);
		}

		public TEntity SingleOrDefault(QueryDelegate query)
		{
			return SingleOrDefault(query, null);
		}

		public TEntity SingleOrDefault(QueryDelegate query, LoadOptionsDelegate loadOptions)
		{
			using (var entityDataContext = NewDataContext())
			{
				var entityQueryable = GetQueryableQuery(query, loadOptions, entityDataContext);
				return entityQueryable.SingleOrDefault();
			}
		}

		public TEntity SingleOrDefaultWhere()
		{
			return SingleOrDefaultWhere(null, null);
		}

		public TEntity SingleOrDefaultWhere(Expression<Func<TEntity, bool>> where)
		{
			return SingleOrDefaultWhere(where, null);
		}

		public TEntity SingleOrDefaultWhere(Expression<Func<TEntity, bool>> where, LoadOptionsDelegate loadOptions)
		{
			using (var entityDataContext = NewDataContext())
			{
				var entityQueryable = GetQueryableWhere(where, loadOptions, entityDataContext);
				return entityQueryable.SingleOrDefault();
			}
		}

		//FirstOrDefault

		public TEntity FirstOrDefault()
		{
			return FirstOrDefault(null, null);
		}

		public TEntity FirstOrDefault(QueryDelegate query)
		{
			return FirstOrDefault(query, null);
		}

		public TEntity FirstOrDefault(QueryDelegate query, LoadOptionsDelegate loadOptions)
		{
			using (var entityDataContext = NewDataContext())
			{
				var entityQueryable = GetQueryableQuery(query, loadOptions, entityDataContext);
				return entityQueryable.FirstOrDefault();
			}
		}

		public TEntity FirstOrDefaultWhere()
		{
			return FirstOrDefaultWhere(null, null);
		}

		public TEntity FirstOrDefaultWhere(Expression<Func<TEntity, bool>> where)
		{
			return FirstOrDefaultWhere(where, null);
		}

		public TEntity FirstOrDefaultWhere(Expression<Func<TEntity, bool>> where, LoadOptionsDelegate loadOptions)
		{
			using (var entityDataContext = NewDataContext())
			{
				var entityQueryable = GetQueryableWhere(where, loadOptions, entityDataContext);
				return entityQueryable.FirstOrDefault();
			}
		}

		//Any

		public bool Any()
		{
			return Any(null, null);
		}

		public bool Any(QueryDelegate query)
		{
			return Any(query, null);
		}

		public bool Any(QueryDelegate query, LoadOptionsDelegate loadOptions)
		{
			using (var entityDataContext = NewDataContext())
			{
				var entityQueryable = GetQueryableQuery(query, loadOptions, entityDataContext);
				return entityQueryable.Any();
			}
		}

		public bool AnyWhere()
		{
			return AnyWhere(null, null);
		}

		public bool AnyWhere(Expression<Func<TEntity, bool>> where)
		{
			return AnyWhere(where, null);
		}

		public bool AnyWhere(Expression<Func<TEntity, bool>> where, LoadOptionsDelegate loadOptions)
		{
			using (var entityDataContext = NewDataContext())
			{
				var entityQueryable = GetQueryableWhere(where, loadOptions, entityDataContext);
				return entityQueryable.Any();
			}
		}
		
		//Count

		public long Count()
		{
			return Count(null, null);
		}

		public long Count(QueryDelegate query)
		{
			return Count(query, null);
		}

		public long Count(QueryDelegate query, LoadOptionsDelegate loadOptions)
		{
			using (var entityDataContext = NewDataContext())
			{
				var entityQueryable = GetQueryableQuery(query, loadOptions, entityDataContext);
				return entityQueryable.LongCount();
			}
		}

		public long CountWhere()
		{
			return CountWhere(null, null);
		}

		public long CountWhere(Expression<Func<TEntity, bool>> where)
		{
			return CountWhere(where, null);
		}

		public long CountWhere(Expression<Func<TEntity, bool>> where, LoadOptionsDelegate loadOptions)
		{
			using (var entityDataContext = NewDataContext())
			{
				var entityQueryable = GetQueryableWhere(where, loadOptions, entityDataContext);
				return entityQueryable.LongCount();
			}
		}

		//Assert

		private static void Fail(string message)
		{
			throw new ApplicationException(message);
		}

		private static void FailIf(bool isTrue, string messegeIfTrue)
		{
			if (isTrue) Fail(messegeIfTrue);
		}
	}
}