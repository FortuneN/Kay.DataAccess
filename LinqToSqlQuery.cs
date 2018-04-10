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

		private static TResult DataContextScope<TResult>(TDataContext dataContext, Func<TDataContext, TResult> logic)
		{
			var dc = dataContext ?? StaticNewDataContext();

			try
			{
				if (dc.Connection.State != ConnectionState.Open) dc.Connection.Open();
				var result = logic.Invoke(dc);
				dc.SubmitChanges();
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

		private static DbCommand CreateCommand(TDataContext dataContext, string query, object parameters)
		{
			var command = dataContext.Connection.CreateCommand();

			command.CommandText = query;
			
			if (dataContext.Transaction != null)
			{
				command.Transaction = dataContext.Transaction;
			}

			if (parameters != null)
			{
				if (parameters is DbParameter[] arrayParameters)
				{
					command.Parameters.AddRange(arrayParameters);
				}
				else if (parameters is Dictionary<string, object> dictionaryParameters)
				{
					foreach (var keyValuePair in dictionaryParameters)
					{
						var parameter = command.CreateParameter();
						parameter.ParameterName = keyValuePair.Key;
						parameter.Value = keyValuePair.Value;
						command.Parameters.Add(parameter);
					}
				}
				else
				{
					throw new Exception("Unknown type of 'parameters' => '" + parameters.GetType() + "'");
				}
			}
			
			return command;
		}

		public TDataContext NewDataContext() => StaticNewDataContext();

		public IEnumerable ExecuteQuery(Type elementType, string query, TDataContext dataContext = null, params object[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");
			return DataContextScope(dataContext, dc => dc.ExecuteQuery(elementType, query, parameters));
		}

		public IEnumerable<TResult> ExecuteQuery<TResult>(string query, TDataContext dataContext = null, params object[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");
			return DataContextScope(dataContext, dc => dc.ExecuteQuery<TResult>(query, parameters));
		}

		public DataTable ExecuteDataTable(string query, TDataContext dataContext = null, Dictionary<string, object> parameters = null)
		{
			FailIf (string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");

			var result = new DataTable();

			return DataContextScope(dataContext, dc =>
			{
				using (var command = CreateCommand(dc, query, parameters))
				using (var dataAdapter = DbProviderFactories.GetFactory(dc.Connection).CreateDataAdapter())
				{
					dataAdapter.SelectCommand = command;
					dataAdapter.Fill(result);
				}

				return result;
			});
		}

		public DbDataReader ExecuteReader(string query, TDataContext dataContext = null, params DbParameter[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");

			return DataContextScope(dataContext, dc =>
			{
				using (var command = CreateCommand(dc, query, parameters))
				{
					return command.ExecuteReader();
				}
			});
		}

		public T ExecuteScalar<T>(string query, TDataContext dataContext = null, params DbParameter[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");

			return DataContextScope(dataContext, dc =>
			{
				using (var command = CreateCommand(dc, query, parameters))
				{
					return (T)command.ExecuteScalar();
				}
			});
		}

		public int ExecuteNonQuery(string query, TDataContext dataContext = null, params DbParameter[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");

			return DataContextScope(dataContext, dc =>
			{
				using (var command = CreateCommand(dc, query, parameters))
				{
					return command.ExecuteNonQuery();
				}
			});
		}

		public DataSet ExecuteDataSet(string query, TDataContext dataContext = null, params DbParameter[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");

			return DataContextScope(dataContext, dc =>
			{
				using (var command = CreateCommand(dc, query, parameters))
				using (var dataAdapter = DbProviderFactories.GetFactory(dc.Connection).CreateDataAdapter())
				{
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