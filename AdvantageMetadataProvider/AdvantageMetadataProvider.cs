using System;
using System.ComponentModel;
using System.Data;
using Advantage.Data.Provider;

namespace ActiveQueryBuilder.Core
{
    /// <summary> Metadata Provider for Advantage DB. </summary>
    [ToolboxItem(true)]
	//[ToolboxBitmap(typeof(AdvantageMetadataProvider))]
	[ToolboxTabNameAttribute("AQB3: Metadata Providers")]
	public class AdvantageMetadataProvider : BaseMetadataProvider
	{
		private AdsConnection _connection;


		[Browsable(true)]
		public override IDbConnection Connection { get { return _connection; } set { SetConnection(value); } }


		static AdvantageMetadataProvider()
		{
			Helpers.MetadataProviderList.RegisterMetadataProvider(typeof(AdvantageMetadataProvider));
		}

		public AdvantageMetadataProvider()
		{
		}

		public AdvantageMetadataProvider(IContainer container) : this()
		{
			container.Add(this);
		}

		private void SetConnection(IDbConnection value)
		{
			if (_connection != value)
			{
				_connection = (AdsConnection) value;
			}
		}

		protected override bool IsConnected()
		{
			if (Connection != null)
			{
				return ((Connection.State & ConnectionState.Open) == ConnectionState.Open);
			}
			
			return false;
		}

		protected override void DoConnect()
		{
			base.DoConnect();

			CheckConnectionSet();

			Connection.Open();
		}

		protected override void DoDisconnect()
		{
			base.DoDisconnect();

			CheckConnectionSet();
			Connection.Close();
		}

		protected override void CheckConnectionSet()
		{
			if (Connection == null)
			{
                throw new QueryBuilderException(ErrorCode.ErrorNoConnectionObject, String.Format(Constants.strNoConnectionObject, "Connection"));
			}
		}

		protected override IDataReader PrepareSQLDatasetInternal(String sql, bool schemaOnly)
		{
			AdsCommand command;
			AdsDataReader reader;

			try
			{
				if (!Connected) Connect();

				command = _connection.CreateCommand();
				command.CommandTimeout = CommandTimeout;
				command.CommandText = sql;

				if (schemaOnly)
				{
					reader = command.ExecuteReader(CommandBehavior.SchemaOnly);
				}
				else
				{
					reader = command.ExecuteReader();
				}
			}
			catch (Exception e)
			{
                throw new QueryBuilderException(ErrorCode.ErrorExecutingQuery,
                    e.Message + "\n\n" + Helpers.Localizer.GetString("strQuery", Constants.strQuery) + "\n" + sql);
			}

			return reader;
		}

		protected override void ExecSQLInternal(string sql)
		{
			AdsCommand command;

			try
			{
				if (!Connected) Connect();

				command = _connection.CreateCommand();
				command.CommandTimeout = CommandTimeout;
				command.CommandText = sql;
				command.ExecuteNonQuery();
			}
			catch (Exception e)
			{
                throw new QueryBuilderException(ErrorCode.ErrorExecutingQuery,
                    e.Message + "\n\n" + Helpers.Localizer.GetString("strQuery", Constants.strQuery) + "\n" + sql);
			}
		}

    	public override void LoadObjects(MetadataList objects, MetadataLoadingOptions loadingOptions)
    	{
    		if (!Connected) Connect();

			try
			{
				// load tables
				DataTable schemaTable = _connection.GetSchema("Tables");

				MetadataObjectFetcherFromDatatable mof = new MetadataObjectFetcherFromDatatable(objects, loadingOptions);
				mof.NameFieldName = "TABLE_NAME";
				mof.TypeFieldName = "TABLE_TYPE";
				mof.TableType = new string[] { "TABLE" };
				mof.SystemTableType = new string[] { "SYSTEM TABLE" };
				mof.Datatable = schemaTable;

				mof.LoadMetadata();

				// load views
				schemaTable = _connection.GetSchema("Views");

				mof = new MetadataObjectFetcherFromDatatable(objects, loadingOptions);

				mof.NameFieldName = "TABLE_NAME";
				mof.DefaultObjectType = MetadataType.View;
				mof.Datatable = schemaTable;

				mof.LoadMetadata();
			}
			catch (Exception exception)
			{
				throw new QueryBuilderException(exception.Message, exception);
			}
    	}

    	public override string GetMetadataProviderDescription()
		{
			return "Advantage Metadata Provider";
		}

		public override bool CanCreateInternalConnection()
		{
			return true;
		}

	    public override void CreateAndBindInternalConnectionObj()
		{
			base.CreateAndBindInternalConnectionObj();
			Connection = new AdsConnection();
			AddInternalConnectionObject(Connection);
		}
	}
}
