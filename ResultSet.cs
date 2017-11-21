using System.Collections.Generic;
using System.Linq;

namespace Kay.DataAccess
{
	public class ResultSet<T>
	{
		//Fields

		private List<T> records;

		private int pageSize;

		private int pageIndex;

		private long recordCount;

		//Properties

		public List<T> Records
		{
			get
			{
				return records;
			}
		}

		public long PageSize
		{
			get
			{
				return pageSize;
			}
		}

		public long PageIndex
		{
			get
			{
				return pageIndex;
			}
		}

		public long RecordCount
		{
			get
			{
				return recordCount;
			}
		}

		public long PageCount
		{
			get
			{
				var pageCount = (recordCount / pageSize);
				if ((recordCount % pageSize) > 0) pageCount += 1;
				return pageCount;
			}
		}

		public long FirstPageIndex
		{
			get
			{
				return 0;
			}
		}

		public long LastPageIndex
		{
			get
			{
				return PageCount - 1;
			}
		}

		public long PreviousPageIndex
		{
			get
			{
				long previousPageIndex = pageIndex - 1;
				if (previousPageIndex < FirstPageIndex) previousPageIndex = FirstPageIndex;
				return previousPageIndex;
			}
		}

		public long NextPageIndex
		{
			get
			{
				long nextPageIndex = pageIndex + 1;
				if (nextPageIndex > LastPageIndex) nextPageIndex = LastPageIndex;
				return nextPageIndex;
			}
		}

		public long PageNumber
		{
			get
			{
				return pageIndex + 1;
			}
		}

		public long FirstPageNumber
		{
			get
			{
				return 1;
			}
		}

		public long LastPageNumber
		{
			get
			{
				return LastPageIndex + 1;
			}
		}

		public long NextPageNumber
		{
			get
			{
				return NextPageIndex + 1;
			}
		}

		public long PreviousPageNumber
		{
			get
			{
				return PreviousPageIndex + 1;
			}
		}

		public bool HasRecords
		{
			get
			{
				return recordCount != 0;
			}
		}

		public bool HasNextPage
		{
			get
			{
				return PageNumber < LastPageNumber;
			}
		}

		public bool HasPreviousPage
		{
			get
			{
				return PageNumber > FirstPageNumber;
			}
		}

		public bool HasMoreThanOnePage
		{
			get
			{
				return PageCount > 1;
			}
		}

		//Constructors

		public ResultSet(IQueryable<T> query, int? pageSize = null, int? pageIndex = null)
		{
			this.pageSize = (!pageSize.HasValue || pageSize <= 0) ? int.MaxValue : pageSize.Value;  // default to <all>
			this.pageIndex = (!pageIndex.HasValue || pageIndex <= 0) ? 0 : pageIndex.Value; // default to <firstIndex>
			this.records = query.Skip(this.pageSize * this.pageIndex).Take(this.pageSize).ToList();
			this.recordCount = query.LongCount();
		}

		public ResultSet(List<T> list)
		{
			this.records = list;
			this.pageIndex = 0;
			this.pageSize = list.Count;
			this.recordCount = list.Count;
		}
	}
}