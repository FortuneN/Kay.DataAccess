using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Linq;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kay.DataAccess
{
	public static class Helper
	{
		public static void Commit<TDataContext>(this TDataContext dc) where TDataContext : DataContext, new()
		{
			dc?.Transaction?.Commit();
		}

		public static void CommitAndDispose<TDataContext>(this TDataContext dc) where TDataContext : DataContext, new()
		{
			dc?.Transaction?.Commit();
			dc?.Dispose();
		}
	}
}
