using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Linq;
using System.Data.Common;
using System.Collections;

namespace Kay.DataAccess
{
	public class LinqToSqlQuery<TDataContext, TConnectionStringReader> where TDataContext : DataContext, new() where TConnectionStringReader : IConnectionStringReader, new()
    {
		private static IConnectionStringReader connectionStringReader = new TConnectionStringReader();

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

		public TDataContext NewDataContext() => StaticNewDataContext();

		public IEnumerable ExecuteQuery(Type elementType, string query, TDataContext dataContext = null, params object[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");
			return DataContextScope(false, dataContext, dc => dc.ExecuteQuery(elementType, query, parameters));
		}

		public IEnumerable<TResult> ExecuteQuery<TResult>(string query, TDataContext dataContext = null, params object[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");
			return DataContextScope(false, dataContext, dc => dc.ExecuteQuery<TResult>(query, parameters));
		}

		public DataTable ExecuteDataTable(string query, TDataContext dataContext = null, Dictionary<string, object> parameters = null)
		{
			FailIf (string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");

			var result = new DataTable();

			return DataContextScope(false, dataContext, dc =>
			{
				if (dc.Connection.State != ConnectionState.Open) dc.Connection.Open();

				using (var command = dc.Connection.CreateCommand())
				using (var dataAdapter = DbProviderFactories.GetFactory(dc.Connection).CreateDataAdapter())
				{
					if (parameters != null)
					{
						foreach (var keyValuePair in parameters)
						{
							var parameter = command.CreateParameter();
							parameter.ParameterName = keyValuePair.Key;
							parameter.Value = keyValuePair.Value;
							command.Parameters.Add(parameter);
						}
					}

					command.CommandText = query;
					dataAdapter.SelectCommand = command;
					dataAdapter.Fill(result);
				}

				return result;
			});
		}

		public DbDataReader ExecuteReader(string query, TDataContext dataContext = null, params DbParameter[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");

			return DataContextScope(false, dataContext, dc =>
			{
				if (dc.Connection.State != ConnectionState.Open) dc.Connection.Open();

				using (var command = dc.Connection.CreateCommand())
				{
					if (parameters != null) command.Parameters.AddRange(parameters);
					command.CommandText = query;
					return command.ExecuteReader();
				}
			});
		}

		public T ExecuteScalar<T>(string query, TDataContext dataContext = null, params DbParameter[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");

			return DataContextScope(false, dataContext, dc =>
			{
				if (dc.Connection.State != ConnectionState.Open) dc.Connection.Open();

				using (var command = dc.Connection.CreateCommand())
				{
					if (parameters != null) command.Parameters.AddRange(parameters);
					command.CommandText = query;
					return (T)command.ExecuteScalar();
				}
			});
		}

		public int ExecuteNonQuery(string query, TDataContext dataContext = null, params DbParameter[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");

			return DataContextScope(true, dataContext, dc =>
			{
				if (dc.Connection.State != ConnectionState.Open) dc.Connection.Open();

				using (var command = dc.Connection.CreateCommand())
				{
					if (parameters != null) command.Parameters.AddRange(parameters);
					command.CommandText = query;
					return command.ExecuteNonQuery();
				}
			});
		}

		public DataSet ExecuteDataSet(string query, TDataContext dataContext = null, params DbParameter[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");

			return DataContextScope(false, dataContext, dc =>
			{
				if (dc.Connection.State != ConnectionState.Open) dc.Connection.Open();
				
				using (var command = dc.Connection.CreateCommand())
				using (var dataAdapter = DbProviderFactories.GetFactory(dc.Connection).CreateDataAdapter())
				{
					if (parameters != null) command.Parameters.AddRange(parameters);
					command.CommandText = query;

					var dataSet = new DataSet();
					dataAdapter.SelectCommand = command;
					dataAdapter.Fill(dataSet);
					return dataSet;
				}
			});
		}

		public DataTable ExecuteDataTable(string query, TDataContext dataContext = null, params DbParameter[] parameters)
		{
			var dataSet = ExecuteDataSet(query, dataContext, parameters);
			if (dataSet.Tables.Count != 0) return dataSet.Tables[0];
			return new DataTable();
		}
		
		//assert

		private static void Fail(string message) => throw new ApplicationException(message);

		private static void FailIf(bool isTrue, string messageIfTrue) { if (isTrue) Fail(messageIfTrue); }
    }
}