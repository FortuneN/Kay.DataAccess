using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
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

		private static TDataContext StaticNewDataContext()
		{
			var dataContext = new TDataContext();
			dataContext.Connection.ConnectionString = connectionStringReader.GetConnectionString();
			if (dataContext.Connection.State != ConnectionState.Open) dataContext.Connection.Open();
			dataContext.Transaction = dataContext.Connection.BeginTransaction();
			return dataContext;
		}

		private static TResult DataContextScope<TResult>(bool submitChanges, TDataContext dataContext, Func<TDataContext, TResult> logic)
		{
			var dc = dataContext ?? StaticNewDataContext();

			try
			{
				var result = logic.Invoke(dc);
				if (submitChanges) dc.SubmitChanges();
				return result;
			}
			finally
			{
				if (dataContext == null)
				{
					dc.Commit();
					dc.Dispose();
				}
			}
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
				foreach (var primaryKeyProperty in entityPrimaryKeyColumnProperties[entityType]) typeWhereSqlList.Add("[" + primaryKeyProperty.Name + "] = {" + typeWhereSqlList.Count + "}");
				entityTableName[entityType] = ((TableAttribute)entityType.GetCustomAttributes(typeof(TableAttribute), true).FirstOrDefault()).Name.Split('.').Last();
				entitySelectSql[entityType] = "SELECT * FROM [" + entityTableName[entityType] + "] WHERE " + string.Join(" AND ", typeWhereSqlList.ToArray());
			}
		}
		
		//Utils

		public string[] AuditFieldsForInsert { set; get; }

		public TDataContext NewDataContext() => StaticNewDataContext();
		
		public delegate IQueryable<TEntity> QueryDelegate(IQueryable<TEntity> query);
		
		public delegate LoadOptions LoadOptionsDelegate(LoadOptions options);

		//Add
		
		public TEntity Add(TEntity entity, TDataContext dataContext = null)
		{
			FailIf(entity == null || entity == DBNull.Value, "Parameter (entity) cannot be null");

			return DataContextScope(true, dataContext, dc =>
			{
				dc.GetTable<TEntity>().InsertOnSubmit(entity);
				return entity;
			});
		}

		public List<TEntity> AddAll(IEnumerable<TEntity> entities, TDataContext dataContext = null)
		{
			FailIf(entities == null || entities.Count() == 0, "Parameter (entities) cannot be null or empty");

			return DataContextScope(true, dataContext, dc =>
			{
				var result = new List<TEntity>();
				foreach (var entity in entities) result.Add(Add(entity, dc));
				return result;
			});
		}

		//Update

		public TEntity Update(TEntity entity, IEnumerable<string> properties = null, TDataContext dataContext = null)
		{
			FailIf(entity == null || entity == DBNull.Value, "Parameter (entity) cannot be null");

			return DataContextScope(true, dataContext, dc =>
			{
				var compositePrimaryKey = GetCompositePrimaryKey(entity);
				var databaseEntity = dc.ExecuteQuery<TEntity>(entitySelectSql[typeof(TEntity)], compositePrimaryKey.Values.ToArray()).SingleOrDefault();

				//update (scalar values only)

				var entityDataContextNamespace = dc.GetType().Namespace;

				foreach (PropertyDescriptor entityPropertyDescriptor in TypeDescriptor.GetProperties(entity))
				{
					var entityPropertyTypeName = entityPropertyDescriptor.PropertyType.FullName;
					var entityPropertyValue = entityPropertyDescriptor.GetValue(entity);

					if (entityPropertyDescriptor.PropertyType.Namespace == entityDataContextNamespace || entityPropertyTypeName.Contains("System.Data.Linq.EntityRef") || entityPropertyTypeName.Contains("System.Data.Linq.EntitySet") || compositePrimaryKey.Keys.Contains(entityPropertyDescriptor.Name))
					{
						continue;
					}

					if (properties != null && !properties.Any(x => string.Equals(x, entityPropertyDescriptor.Name, StringComparison.OrdinalIgnoreCase)))
					{
						continue;
					}

					if (AuditFieldsForInsert != null && AuditFieldsForInsert.Any(x => string.Equals(x, entityPropertyDescriptor.Name, StringComparison.OrdinalIgnoreCase)))
					{
						continue;
					}

					//TODO:The generic implementation of this is done (AuditFieldsForInsert check above). Retain this for Praxis use for now : 10 April 2018
					if (entityPropertyDescriptor.Name.Equals("DateCreated", StringComparison.OrdinalIgnoreCase) || entityPropertyDescriptor.Name.Equals("UserCreated", StringComparison.OrdinalIgnoreCase))
					{
						continue;
					}

					entityPropertyDescriptor.SetValue(databaseEntity, entityPropertyValue);
				}
				
				//return

				return databaseEntity;
			});
		}

		public List<TEntity> UpdateAll(IEnumerable<TEntity> entities, TDataContext dataContext = null)
		{
			FailIf(entities == null, "Parameter (entities) cannot be null or empty");

			return DataContextScope(true, dataContext, dc =>
			{
				var result = new List<TEntity>();
				foreach (var entity in entities) result.Add(Update(entity, null, dc));
				return result;
			});
		}
		
		//public IEnumerable<TEntity> UpdateWhere(Expression<Func<TEntity, bool>> where, Expression<Func<TEntity, TEntity>> update = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => UpdateAll(QueryableWhere(where, null, dc).ToList(), dc));

		//public IEnumerable<TEntity> UpdateQuery(QueryDelegate query = null, Expression<Func<TEntity, TEntity>> update = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => UpdateAll(QueryableQuery(query, null, dc).ToList(), dc));
		
		//Delete

		public TEntity DeleteByPrimaryKey(Dictionary<string, object> compositePrimaryKey, TDataContext dataContext = null)
		{
			FailIf(compositePrimaryKey == null || compositePrimaryKey.Count() == 0, "Parameter (compositePrimaryKey) cannot be null or empty");

			return DataContextScope(true, dataContext, dc =>
			{
				var persistedEntity = dc.ExecuteQuery<TEntity>(entitySelectSql[typeof(TEntity)], compositePrimaryKey.Values.ToArray()).SingleOrDefault();
				dc.GetTable<TEntity>().DeleteOnSubmit(persistedEntity);
				return persistedEntity;
			});
		}

		public TEntity DeleteByPrimaryKey(object primaryKey, TDataContext dataContext = null)
		{
			FailIf(primaryKey == null || primaryKey == DBNull.Value, "Parameter (primaryKey) cannot be null");

			return DataContextScope(true, dataContext, dc =>
			{
				var primaryKeyColumnProperties = entityPrimaryKeyColumnProperties[typeof(TEntity)];
				var primaryKeyName = primaryKeyColumnProperties.Select(x => x.Name).SingleOrDefault();
				var compositePrimaryKey = new Dictionary<string, object> { { primaryKeyName, primaryKey } };
				return DeleteByPrimaryKey(compositePrimaryKey, dc);
			});
		}

		public TEntity Delete(TEntity entity, TDataContext dataContext = null)
		{
			FailIf(entity == null || entity == DBNull.Value, "Parameter (entity) cannot be null");

			return DataContextScope(true, dataContext, dc => DeleteByPrimaryKey(GetCompositePrimaryKey(entity), dc));
		}

		public IEnumerable<TEntity> DeleteAll(IEnumerable<TEntity> entities, TDataContext dataContext = null)
		{
			FailIf(entities == null, "Parameter (entities) cannot be null");

			return DataContextScope(true, dataContext, dc =>
			{
				var result = new List<TEntity>();
				foreach (var entity in entities) result.Add(Delete(entity, dc));
				return result;
			});
		}

		public IEnumerable<TEntity> DeleteWhere(Expression<Func<TEntity, bool>> where, TDataContext dataContext = null) => DataContextScope(true, dataContext, dc => DeleteAll(QueryableWhere(where, null, dc).ToList(), dc));
		
		public IEnumerable<TEntity> DeleteQuery(QueryDelegate query = null, TDataContext dataContext = null) => DataContextScope(true, dataContext, dc => DeleteAll(QueryableQuery(query, null, dc).ToList(), dc));

		//GetByPrimaryKey

		public TEntity GetByPrimaryKey(Dictionary<string, object> compositePrimaryKey, TDataContext dataContext = null)
		{
			FailIf(compositePrimaryKey == null || compositePrimaryKey.Count() == 0, "Parameter (compositePrimaryKey) cannot be null or empty");

			return DataContextScope(true, dataContext, dc => dc.ExecuteQuery<TEntity>(entitySelectSql[typeof(TEntity)], compositePrimaryKey.Values.ToArray()).SingleOrDefault());
		}

		public TEntity GetByPrimaryKey(object primaryKey, TDataContext dataContext = null)
		{
			FailIf(primaryKey == null || primaryKey == DBNull.Value, "Parameter (primaryKey) cannot be null or DBNull");

			return DataContextScope(true, dataContext, dc =>
			{
				var primaryKeyColumnProperties = entityPrimaryKeyColumnProperties[typeof(TEntity)];
				var primaryKeyName = primaryKeyColumnProperties.Select(x => x.Name).SingleOrDefault();
				var compositePrimaryKey = new Dictionary<string, object> { { primaryKeyName, primaryKey } };
				return GetByPrimaryKey(compositePrimaryKey, dc);
			});
		}
		
		//Queryable

		public IQueryable<TEntity> Queryable(LoadOptionsDelegate options = null, TDataContext dataContext = null)
		{
			FailIf(dataContext == null, "Parameter (dataContext) cannot be null");
			var queryable = dataContext.GetTable<TEntity>().AsQueryable();
			if (options != null) dataContext.LoadOptions = options(new LoadOptions()).Options;
			return queryable;
		}

		public IQueryable<TEntity> QueryableQuery(QueryDelegate query = null, LoadOptionsDelegate options = null, TDataContext dataContext = null)
		{
			FailIf(dataContext == null, "Parameter (dataContext) cannot be null");
			var queryable = Queryable(options, dataContext);
			return query != null ? query(queryable) : queryable;
		}

		public IQueryable<TEntity> QueryableWhere(Expression<Func<TEntity, bool>> where = null, LoadOptionsDelegate options = null, TDataContext dataContext = null)
		{
			FailIf(dataContext == null, "Parameter (dataContext) cannot be null");
			var queryable = Queryable(options, dataContext);
			return where != null ? queryable.Where(where) : queryable;
		}

		//ResultSet
		
		public ResultSet<TEntity> ResultSet(LoadOptionsDelegate options = null, int? pageSize = null, int? pageIndex = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => new ResultSet<TEntity>(Queryable(options, dc), pageSize, pageIndex));
		
		public ResultSet<TEntity> ResultSetQuery(QueryDelegate query = null, LoadOptionsDelegate options = null, int? pageSize = null, int? pageIndex = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => new ResultSet<TEntity>(QueryableQuery(query, options, dc), pageSize, pageIndex));

		public ResultSet<TEntity> ResultSetWhere(Expression<Func<TEntity, bool>> where = null, LoadOptionsDelegate options = null, int? pageSize = null, int? pageIndex = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => new ResultSet<TEntity>(QueryableWhere(where, options, dc), pageSize, pageIndex));

		//SingleOrDefault
		
		public TEntity SingleOrDefault(LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => Queryable(options, dc).SingleOrDefault());
		
		public TEntity SingleOrDefaultQuery(QueryDelegate query = null, LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => QueryableQuery(query, options, dc).SingleOrDefault());

		public TEntity SingleOrDefaultWhere(Expression<Func<TEntity, bool>> where = null, LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => QueryableWhere(where, options, dc).SingleOrDefault());

		//FirstOrDefault
		
		public TEntity FirstOrDefault(LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => Queryable(options, dc).FirstOrDefault());
		
		public TEntity FirstOrDefaultQuery(QueryDelegate query = null, LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => QueryableQuery(query, options, dc).FirstOrDefault());
		
		public TEntity FirstOrDefaultWhere(Expression<Func<TEntity, bool>> where = null, LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => QueryableWhere(where, options, dc).FirstOrDefault());

		//LastOrDefault
		
		public TEntity LastOrDefault(LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => Queryable(options, dc).LastOrDefault());
		
		public TEntity LastOrDefaultQuery(QueryDelegate query = null, LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => QueryableQuery(query, options, dc).LastOrDefault());

		public TEntity LastOrDefaultWhere(Expression<Func<TEntity, bool>> where = null, LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => QueryableWhere(where, options, dc).LastOrDefault());

		//Any
		
		public bool Any(TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => Queryable(null, dc).Any());
		
		public bool AnyQuery(QueryDelegate query = null, LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => QueryableQuery(query, options, dc).Any());

		public bool AnyWhere(Expression<Func<TEntity, bool>> where = null, LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => QueryableWhere(where, options, dc).Any());

		//Count
		
		public long Count(TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => Queryable(null, dc).LongCount());
		
		public long CountQuery(QueryDelegate query = null, LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => QueryableQuery(query, options, dc).LongCount());

		public long CountWhere(Expression<Func<TEntity, bool>> where = null, LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => QueryableWhere(where, options, dc).LongCount());

		//Array
		
		public TEntity[] Array(LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => Queryable(options, dc).ToArray());
		
		public TEntity[] ArrayQuery(QueryDelegate query = null, LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => QueryableQuery(query, options, dc).ToArray());

		public TEntity[] ArrayWhere(Expression<Func<TEntity, bool>> where = null, LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => QueryableWhere(where, options, dc).ToArray());

		//List
		
		public List<TEntity> List(LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => Queryable(options, dc).ToList());
		
		public List<TEntity> ListQuery(QueryDelegate query = null, LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => QueryableQuery(query, options, dc).ToList());

		public List<TEntity> ListWhere(Expression<Func<TEntity, bool>> where = null, LoadOptionsDelegate options = null, TDataContext dataContext = null) => DataContextScope(false, dataContext, dc => QueryableWhere(where, options, dc).ToList());

		//Assert
		
		private static void Fail(string message) => throw new ApplicationException(message);

		private static void FailIf(bool isTrue, string messageIfTrue) { if (isTrue) Fail(messageIfTrue); }
	}
}