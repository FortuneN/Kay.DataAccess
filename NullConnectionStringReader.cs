namespace Kay.DataAccess
{
	public class NullConnectionStringReader : IConnectionStringReader
	{
		public string GetConnectionString()
		{
			return null;
		}
	}
}
