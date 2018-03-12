using System;
using System.Data;
using System.Data.SqlClient;
using System.ComponentModel;

namespace ActiveQueryBuilder.Core
{
	/// <summary> Metadata Provider for Microsoft SQL Server. </summary>
	[ToolboxItem(true)]
	//[ToolboxBitmap(typeof(MSSQLMetadataProvider))]
	[ToolboxTabNameAttribute("AQB3: Metadata Providers")]
	public class MSSQLMetadataProvider : BaseMetadataProvider
	{
		private SqlConnection _connection;

		[Browsable(true)]
		public override IDbConnection Connection { get { return _connection; } set { SetConnection(value); } }

		static MSSQLMetadataProvider()
		{
			Helpers.MetadataProviderList.RegisterMetadataProvider(typeof(MSSQLMetadataProvider));
		}

		public MSSQLMetadataProvider()
		{
		}

		public MSSQLMetadataProvider(IContainer container) : this()
		{
			container.Add(this);
		}

		private void SetConnection(IDbConnection value)
		{
			if (_connection != value)
			{
				_connection = (SqlConnection) value;
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
			SqlCommand command;
			SqlDataReader reader;

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
			SqlCommand command;

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

		public override void LoadDatabases(MetadataList databases, MetadataLoadingOptions loadingOptions)
		{
			if (databases.Parent.Server == null)
			{
				if (!Connected) Connect();

				string currentDatabase = Connection.Database;
				if (!String.IsNullOrEmpty(currentDatabase))
				{
					MetadataNamespace database = databases.FindItem<MetadataNamespace>(currentDatabase, MetadataType.Database);
					if (database == null)
					{
						database = new MetadataNamespace(databases, MetadataType.Database);
						database.Name = currentDatabase;
						databases.Add(database);
					}

					database.Default = true;
				}
			}
		}

		public override void LoadSchemas(MetadataList schemas, MetadataLoadingOptions loadingOptions)
		{
			MetadataNamespace database = schemas.Parent.Database;
			if (schemas.Parent.Server == null && database != null)
			{
				if (!Connected) Connect();

				try
				{
					string[] restrictions = new string[1];
					restrictions[0] = database.Name;

					using (DataTable schemaTable = _connection.GetSchema("Tables", restrictions))
					{
						using (MetadataNamespacesFetcherFromDatatable mdf = 
							new MetadataNamespacesFetcherFromDatatable(schemas,MetadataType.Schema,loadingOptions))
						{
							mdf.Datatable = schemaTable;
							mdf.NameFieldName = "TABLE_SCHEMA";

							mdf.LoadMetadata();
						}
					}

					using (DataTable schemaTable = _connection.GetSchema("Views", restrictions))
					{
						using (MetadataNamespacesFetcherFromDatatable mdf =
							new MetadataNamespacesFetcherFromDatatable(schemas, MetadataType.Schema, loadingOptions))
						{
							mdf.Datatable = schemaTable;
							mdf.NameFieldName = "TABLE_SCHEMA";

							mdf.LoadMetadata();
						}
					}
				}
				catch (Exception exception)
				{
					throw new QueryBuilderException(exception.Message, exception);
				}
			}
		}

		public override void LoadObjects(MetadataList objects, MetadataLoadingOptions loadingOptions)
		{
			MetadataNamespace database = objects.Parent.Database;
			MetadataNamespace schema = objects.Parent.Schema;
			if (objects.Parent.Server == null && database != null && schema != null)
			{
				if (!Connected) Connect();

				try
				{
					string[] restrictions = new string[2];
					restrictions[0] = database.Name;
					restrictions[1] = schema.Name;

					using (DataTable schemaTable = _connection.GetSchema("Tables", restrictions))
					{
						MetadataObjectFetcherFromDatatable mof = new MetadataObjectFetcherFromDatatable(objects, loadingOptions);
						
						mof.Datatable = schemaTable;
						mof.NameFieldName = "TABLE_NAME";

						mof.TypeFieldName = "TABLE_TYPE";
						mof.TableType = "BASE TABLE";
						mof.ViewType = "VIEW";

						mof.DefaultObjectType = MetadataType.Table;

						mof.LoadMetadata();
					}
				}
				catch (Exception exception)
				{
					throw new QueryBuilderException(exception.Message, exception);
				}
			}
		}

		public override void LoadFields(MetadataList fields, MetadataLoadingOptions loadingOptions)
		{
			MetadataItem obj = fields.Parent.Object;
			MetadataNamespace schema = obj.Schema;
			MetadataNamespace database = obj.Database;

			if (schema != null && database != null && obj.Server == null)
			{
				string[] restrictions = new string[3];
				restrictions[0] = database.Name;
				restrictions[1] = schema.Name;
				restrictions[2] = obj.Name;

				using (DataTable dataTable = _connection.GetSchema("Columns", restrictions))
				{

					MetadataFieldsFetcherFromDatatable mff = new MetadataFieldsFetcherFromDatatable(fields.SQLContext);

					mff.Datatable = dataTable;

					mff.NameFieldName = "COLUMN_NAME";

					mff.NullableFieldName = "IS_NULLABLE";
					mff.NullableValue = "YES";

					mff.ServerTypeFieldName = "DATA_TYPE";
					mff.SizeFieldName = "CHARACTER_MAXIMUM_LENGTH";
					mff.PrecisionFieldName = "NUMERIC_PRECISION";
					mff.ScaleFieldName = "NUMERIC_SCALE";

					mff.DefaultValueFieldName = "COLUMN_DEFAULT";

					mff.LoadFields(fields);
				}

				base.LoadFields(fields, loadingOptions);
			}
		}

		public override string GetMetadataProviderDescription()
		{
			return "MSSQL Metadata Provider";
		}

		public override bool CanCreateInternalConnection()
		{
			return true;
		}

		public override void CreateAndBindInternalConnectionObj()
		{
			base.CreateAndBindInternalConnectionObj();
			
			Connection = new SqlConnection();

			AddInternalConnectionObject(Connection);
		}
	}
}
