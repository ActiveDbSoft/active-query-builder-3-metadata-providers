using System;
using System.Data;
using System.ComponentModel;
using FirebirdSql.Data.FirebirdClient;

namespace ActiveQueryBuilder.Core
{
    /// <summary> Metadata Provider for Firebird databases. </summary>
    [ToolboxItem(true)]
	//[ToolboxBitmap(typeof(FirebirdMetadataProvider))]
	[ToolboxTabNameAttribute("AQB3: Metadata Providers")]
	public class FirebirdMetadataProvider : BaseMetadataProvider
	{
		private FbConnection _connection;

		[Browsable(true)]
		public override IDbConnection Connection { get { return _connection; } set { SetConnection(value); } }

		static FirebirdMetadataProvider()
		{
			Helpers.MetadataProviderList.RegisterMetadataProvider(typeof(FirebirdMetadataProvider));
		}

		public FirebirdMetadataProvider()
		{
		}

		public FirebirdMetadataProvider(IContainer container) : this()
		{
			container.Add(this);
		}

		private void SetConnection(IDbConnection value)
		{
			if (_connection != value)
			{
				_connection = (FbConnection) value;
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
			FbCommand command;
			FbDataReader reader;

			try
			{
				if (!Connected) Connect();

				command = _connection.CreateCommand();
				command.CommandTimeout = CommandTimeout;
				command.CommandText = sql;

				if (schemaOnly)
				{
					reader = command.ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo);
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
			FbCommand command;

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
				// load tables and views
				DataTable schemaTable = _connection.GetSchema("Tables", null);

				MetadataObjectFetcherFromDatatable mof = new MetadataObjectFetcherFromDatatable(objects, loadingOptions);
				mof.NameFieldName = "TABLE_NAME";
				mof.TypeFieldName = "TABLE_TYPE";
				mof.TableType = new string[] { "TABLE" };
				mof.SystemTableType = new string[] { "SYSTEM_TABLE" };
				mof.ViewType = new string[] { "VIEW" };
				mof.TrimSpaces = true;
				mof.Datatable = schemaTable;

				mof.LoadMetadata();

				// load procedures
				schemaTable = _connection.GetSchema("Procedures", null);

				mof = new MetadataObjectFetcherFromDatatable(objects, loadingOptions);
				mof.NameFieldName = "PROCEDURE_NAME";
				mof.SystemFieldName = "IS_SYSTEM_PROCEDURE";
				mof.SystemFieldValue = 1;
				mof.TrimSpaces = true;
				mof.Datatable = schemaTable;
				mof.DefaultObjectType = MetadataType.Procedure;

				mof.LoadMetadata();
			}
			catch (Exception exception)
			{
				throw new QueryBuilderException(exception.Message, exception);
			}
		}

		public override void LoadForeignKeys(MetadataList foreignKeys, MetadataLoadingOptions loadingOptions)
		{
			if (!Connected) Connect();

			// load FKs
			try
			{
				MetadataItem obj = foreignKeys.Parent.Object;

				string[] restrictions = new string[3];
				restrictions[2] = obj.Name;

				DataTable schemaTable = _connection.GetSchema("ForeignKeyColumns", restrictions);

				MetadataForeignKeysFetcherFromDatatable mrf = new MetadataForeignKeysFetcherFromDatatable(foreignKeys, loadingOptions);
				mrf.PkNameFieldName = "REFERENCED_TABLE_NAME";
				mrf.PkFieldName = "REFERENCED_COLUMN_NAME";
				mrf.FkFieldName = "COLUMN_NAME";
				mrf.OrdinalFieldName = "ORDINAL_POSITION";
				mrf.TrimSpaces = true;
				mrf.Datatable = schemaTable;

				mrf.LoadMetadata();
			}
			catch (Exception exception)
			{
				throw new QueryBuilderException(exception.Message, exception);
			}
		}

		public override string GetMetadataProviderDescription()
		{
			return "Firebird Metadata Provider";
		}

		public override bool CanCreateInternalConnection()
		{
			return true;
		}

	    public override void CreateAndBindInternalConnectionObj()
		{
			base.CreateAndBindInternalConnectionObj();
			Connection = new FbConnection();
			AddInternalConnectionObject(Connection);
		}
	}
}
