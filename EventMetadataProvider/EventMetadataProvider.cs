using System;
using System.ComponentModel;
using System.Data;

namespace ActiveQueryBuilder.Core
{
	public delegate void ExecSQLEventHandler(BaseMetadataProvider metadataProvider, string sqlQuery, bool schemaOnly, out IDataReader dataReader);

    /// <summary>Metadata provider to work with unsupported .NET data providers or custom data sources.</summary>
    /// <remarks>
    /// 	<para>Use this event on working with unsupported .NET data provider which is capable to execute SQL queries and to return the IDataReader interface in result.</para>
    /// 	<para>See <see cref="ExecSQL"/> event for details.</para>
    /// </remarks>
	[ToolboxItem(true)]
	//[ToolboxBitmap(typeof(EventMetadataProvider))]
	[ToolboxTabNameAttribute("AQB3: Metadata Providers")]
	public class EventMetadataProvider : BaseMetadataProvider
    {
		/// <summary>Invoked when the Event Metadata Provider is requested to execute the SQL statement.</summary>
        /// <remarks>The metadataProvider parameter refers to the metadata provider invoked the event. The sql parameter contains the query to be executed. The
        /// schemaOnly flag indicates that there's no need for data retrieval for this query. Return the IDataReader interface through the dataReader parameter.</remarks>
        /// <example>
        /// 	<code title="ExecSQL event sample" description="" lang="CS">
        /// private void EventMetadataProvider_ExecSQL(BaseMetadataProvider metadataProvider, string sql, bool schemaOnly, out IDataReader dataReader)
        /// {
        ///     dataReader = null;
        ///  
        ///     if (dbConnection != null)
        ///     {
        ///         IDbCommand command = dbConnection.CreateCommand();
        ///         command.CommandText = sql;
        ///         dataReader = command.ExecuteReader();
        ///     }
        /// }</code>
        /// </example>
		[Description("Occurs when the query builder requests the result of the provided SQL query. (Handle this event if your custom data connection supports query execution.)")]
		public new event ExecSQLEventHandler ExecSQL;


		[Browsable(false)]
		[EditorBrowsable(EditorBrowsableState.Never)]
		[DesignerSerializationVisibility(DesignerSerializationVisibility.Hidden)]
		public override IDbConnection Connection
		{
			get { throw new QueryBuilderException("This property is not applicable for this Metadata Provider."); }
			set { throw new QueryBuilderException("This property is not applicable for this Metadata Provider."); }
		}
	

		static EventMetadataProvider()
		{
			Helpers.MetadataProviderList.RegisterMetadataProvider(typeof(EventMetadataProvider));
		}

		public EventMetadataProvider()
		{
		}

		public EventMetadataProvider(IContainer container) : this()
		{
			container.Add(this);
		}

		public override string GetMetadataProviderDescription()
		{
			return "Event Metadata Provider";
		}

		protected override bool GetCanExecSQL()
		{
			return (ExecSQL != null);
		}

		protected override IDataReader PrepareSQLDatasetInternal(String sql, bool schemaOnly)
		{
			IDataReader result;

			if (ExecSQL != null)
			{
				try
				{
					ExecSQL(this, sql, schemaOnly, out result);
				}
				catch (Exception e)
				{
                throw new QueryBuilderException(ErrorCode.ErrorExecutingQuery,
                    e.Message + "\n\n" + Helpers.Localizer.GetString("strQuery", Constants.strQuery) + "\n" + sql);
				}
			}
			else
			{
				result = null;
			}

			return result;
		}
    }
}
