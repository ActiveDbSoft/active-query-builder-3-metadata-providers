using System;
using System.Data;
using System.ComponentModel;
using IBM.Data.DB2;

namespace ActiveQueryBuilder.Core
{
    /// <summary> Metadata Provider for IBM DB2. </summary>
    [ToolboxItem(true)]
	//[ToolboxBitmap(typeof(DB2MetadataProvider))]
	[ToolboxTabNameAttribute("AQB3: Metadata Providers")]
	public class DB2MetadataProvider : BaseMetadataProvider
	{
		private DB2Connection _connection;
		private SQLQualifiedName _defaultDatabaseName;

		[Browsable(true)]
		public override IDbConnection Connection { get { return _connection; } set { SetConnection(value); } }


		static DB2MetadataProvider()
		{
			Helpers.MetadataProviderList.RegisterMetadataProvider(typeof(DB2MetadataProvider));
		}

		public DB2MetadataProvider()
		{
		}

		public DB2MetadataProvider(IContainer container) : this()
		{
			container.Add(this);
		}

		protected override void Dispose(bool disposing)
		{
			if (_defaultDatabaseName != null)
			{
				_defaultDatabaseName.Dispose();
				_defaultDatabaseName = null;
			}

			base.Dispose(disposing);
		}

		private void SetConnection(IDbConnection value)
		{
			if (_connection != value)
			{
				_connection = (DB2Connection) value;
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
			DB2Command command;
			DB2DataReader reader;

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
			DB2Command command;

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
			SQLContext sqlContext = objects.SQLContext;

			try
			{
				// load tables and views
				string[] restrictions = new string[2];

				if (sqlContext.SyntaxProvider.IsSupportDatabases())
				{
					MetadataNamespace database = objects.Parent.Database;
					if (database != null)
						restrictions[0] = database.Name;
					else
						return;
				}
				else
				{
					restrictions[0] = null;
				}

				if (sqlContext.SyntaxProvider.IsSupportSchemas())
				{
					MetadataNamespace schema = objects.Parent.Schema;
					if (schema != null)
						restrictions[1] = schema.Name;
					else
						return;
				}
				else
				{
					restrictions[1] = null;
				}

				DataTable schemaTable = _connection.GetSchema(DB2MetaDataCollectionNames.Tables, restrictions);

				MetadataObjectFetcherFromDatatable mof = new MetadataObjectFetcherFromDatatable(objects, loadingOptions);
				
				mof.NameFieldName = "TABLE_NAME";
				mof.TypeFieldName = "TABLE_TYPE";
				mof.TableType = new string[] { "TABLE" };
				mof.SystemTableType = new string[] { "SYSTEM TABLE" };
				mof.ViewType = new string[] { "VIEW", "MATERIALIZED QUERY TABLE" };
				mof.SynonymType = "ALIAS";
				mof.Datatable = schemaTable;

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

			MetadataItem obj = foreignKeys.Parent.Object;
			MetadataItem schema = obj.Schema;
			MetadataItem database = obj.Database;

			try
			{
				string[] restrictions = new string[6];
				restrictions[3] = database != null ? database.Name : null;
				restrictions[4] = schema != null ? schema.Name : null;
				restrictions[5] = obj.Name;

				DataTable schemaTable = _connection.GetSchema(DB2MetaDataCollectionNames.ForeignKeys, restrictions);

				MetadataForeignKeysFetcherFromDatatable mrf = new MetadataForeignKeysFetcherFromDatatable(foreignKeys, loadingOptions);

				mrf.PkSchemaFieldName = "PKTABLE_SCHEMA";
				mrf.PkDatabaseFieldName = "PKTABLE_CATALOG";
				mrf.PkNameFieldName = "PKTABLE_NAME";
				mrf.FkFieldName = "FKTABLE_NAME";
				mrf.PkFieldName = "PKCOLUMN_NAME";
				mrf.FkFieldName = "FKCOLUMN_NAME";
				mrf.OrdinalFieldName = "KEY_SEQ";
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
			return "IBM DB2 Metadata Provider";
		}

		public override bool CanCreateInternalConnection()
		{
			return true;
		}

	    public override void CreateAndBindInternalConnectionObj()
		{
			base.CreateAndBindInternalConnectionObj();

			Connection = new DB2Connection();

			AddInternalConnectionObject(Connection);
		}
	}
}
