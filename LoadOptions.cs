using System;
using System.Linq.Expressions;
using System.Data.Linq;

namespace Kay.DataAccess
{
	/// <summary>
	/// TODO: Update summary.
	/// </summary>
	public class LoadOptions
    {
        private DataLoadOptions dataLoadOptions = new DataLoadOptions();
        
        public DataLoadOptions Options { get { return dataLoadOptions; } }

        public LoadOptions AssociateWith<T>(Expression<Func<T, object>> expression)
        {
            dataLoadOptions.AssociateWith<T>(expression);
            return this;
        }

        public LoadOptions AssociateWith(LambdaExpression expression)
        {
            dataLoadOptions.AssociateWith(expression);
            return this;
        }

        public LoadOptions LoadWith<T>(Expression<Func<T, object>> expression)
        {
            dataLoadOptions.LoadWith<T>(expression);
            return this;
        }

        public LoadOptions LoadWith(LambdaExpression expression)
        {
            dataLoadOptions.LoadWith(expression);
            return this;
        }
    }
}
