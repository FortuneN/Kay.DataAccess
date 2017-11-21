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

		public IEnumerable ExecuteQuery(Type elementType, string query, params object[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");

			using (var entityDataContext = NewDataContext())
			{
				return entityDataContext.ExecuteQuery(elementType, query, parameters);
			}
		}

		public IEnumerable<TResult> ExecuteQuery<TResult>(string query, params object[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");

			using (var entityDataContext = NewDataContext())
			{
				return entityDataContext.ExecuteQuery<TResult>(query, parameters);
			}
		}

		public DataTable ExecuteDataTable(string query, Dictionary<string, object> parameters = null)
		{
			FailIf (string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");

			DataTable result = new DataTable();
			
			using (var entityDataContext = NewDataContext())
			using (var connection = entityDataContext.Connection)
			using (var command = connection.CreateCommand())
			using (var dataAdapter = DbProviderFactories.GetFactory(connection).CreateDataAdapter())
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
		}

		public DbDataReader ExecuteReader(string query, params DbParameter[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");

			using (var entityDataContext = NewDataContext())
			using (var connection = entityDataContext.Connection)
			using (var command = connection.CreateCommand())
			{
                connection.Open();
				if (parameters != null) command.Parameters.AddRange(parameters);
				command.CommandText = query;
				return command.ExecuteReader();
			}
		}

		public object ExecuteScalar(string query, params DbParameter[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");

			using (var entityDataContext = NewDataContext())
			using (var connection = entityDataContext.Connection)
			using (var command = connection.CreateCommand())
			{

                connection.Open();
				if (parameters != null) command.Parameters.AddRange(parameters);
				command.CommandText = query;
				return command.ExecuteScalar();
			}
		}

		public int ExecuteNonQuery(string query, params DbParameter[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");

			using (var entityDataContext = NewDataContext())
			using (var connection = entityDataContext.Connection)
			using (var command = connection.CreateCommand())
			{
				if (parameters != null) command.Parameters.AddRange(parameters);
				command.CommandText = query;
				return command.ExecuteNonQuery();
			}
		}

		public DataSet ExecuteDataSet(string query, params DbParameter[] parameters)
		{
			FailIf(string.IsNullOrWhiteSpace(query), "Parameter 'query' is required");

			using (var entityDataContext = NewDataContext())
			using (var connection = entityDataContext.Connection)
			using (var command = connection.CreateCommand())
			using (var dataAdapter = DbProviderFactories.GetFactory(connection).CreateDataAdapter())
			{
				if (parameters != null) command.Parameters.AddRange(parameters);
				command.CommandText = query;

				DataSet dataSet = new DataSet();
				dataAdapter.SelectCommand = command;
				dataAdapter.Fill(dataSet);
				return dataSet;
			}
		}

		public DataTable ExecuteDataTable(string query, params DbParameter[] parameters)
		{
			DataSet dataSet = ExecuteDataSet(query, parameters);
			if (dataSet.Tables.Count != 0) return dataSet.Tables[0];
			return new DataTable();
		}

		//data-context

		public TDataContext NewDataContext()
		{
			var dataContext = new TDataContext();
			dataContext.Connection.ConnectionString = connectionStringReader.GetConnectionString();
			return dataContext;
		}

		//assert

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