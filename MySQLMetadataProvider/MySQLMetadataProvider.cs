using System;
using System.Data;
using System.ComponentModel;
using MySql.Data.MySqlClient;

namespace ActiveQueryBuilder.Core
{
    /// <summary> Metadata Provider for MySQL Server. </summary>
    [ToolboxItem(true)]
	//[ToolboxBitmap(typeof(MySQLMetadataProvider))]
	[ToolboxTabNameAttribute("AQB3: Metadata Providers")]
	public class MySQLMetadataProvider : BaseMetadataProvider
	{
		private MySqlConnection _connection;
		private SQLQualifiedName _defaultDatabaseName;

		[Browsable(true)]
		public override IDbConnection Connection { get { return _connection; } set { SetConnection(value); } }


		static MySQLMetadataProvider()
		{
			Helpers.MetadataProviderList.RegisterMetadataProvider(typeof(MySQLMetadataProvider));
		}

		public MySQLMetadataProvider()
		{
		}

		public MySQLMetadataProvider(IContainer container) : this()
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
				_connection = (MySqlConnection) value;
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
			MySqlCommand command;
			MySqlDataReader reader;

			try
			{
				if (!Connected)
				{
					Connect();
				}

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
			MySqlCommand command;

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
				// load tables
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

				DataTable schemaTable = _connection.GetSchema("TABLES", restrictions);

				MetadataObjectFetcherFromDatatable mof = new MetadataObjectFetcherFromDatatable(objects, loadingOptions);
				mof.NameFieldName = "TABLE_NAME";
				mof.TypeFieldName = "TABLE_TYPE";
				mof.TableType = new string[] { "BASE TABLE" };
				mof.SystemTableType = new string[] { "SYSTEM TABLE" };
				mof.Datatable = schemaTable;

				mof.LoadMetadata();

				// load views
				schemaTable = _connection.GetSchema("VIEWS", null);
				mof = new MetadataObjectFetcherFromDatatable(objects, loadingOptions);
				mof.NameFieldName = "TABLE_NAME";
				mof.Datatable = schemaTable;
				mof.DefaultObjectType = MetadataType.View;

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
				MetadataItem database = obj.Database;

				string[] restrictions = new string[2];
				restrictions[0] = database != null ? database.Name : string.Empty;
				restrictions[1] =  obj.Name;

				DataTable schemaTable = _connection.GetSchema("Foreign Key Columns", restrictions);

				MetadataForeignKeysFetcherFromDatatable mrf = new MetadataForeignKeysFetcherFromDatatable(foreignKeys, loadingOptions);

				if (foreignKeys.SQLContext.SyntaxProvider.IsSupportSchemas())
					mrf.PkSchemaFieldName = "TABLE_SCHEMA";
				else
					mrf.PkDatabaseFieldName = "TABLE_SCHEMA";

				mrf.PkNameFieldName = "TABLE_NAME";
				mrf.PkFieldName = "COLUMN_NAME";
				mrf.FkFieldName = "REFERENCED_COLUMN_NAME";
				mrf.OrdinalFieldName = "ORDINAL_POSITION";
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
			return "MySQL Metadata Provider";
		}

		public override bool CanCreateInternalConnection()
		{
			return true;
		}

	    public override void CreateAndBindInternalConnectionObj()
		{
			base.CreateAndBindInternalConnectionObj();
			Connection = new MySqlConnection();
			AddInternalConnectionObject(Connection);
		}
	}
}
