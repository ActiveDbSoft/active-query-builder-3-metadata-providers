using System;
using System.Data;
using System.Data.OleDb;
using System.ComponentModel;

namespace ActiveQueryBuilder.Core
{
	/// <summary> Metadata Provider for OLE DB Data Sources. </summary>
	[ToolboxItem(true)]
	//[ToolboxBitmap(typeof(OLEDBMetadataProvider))]
	[ToolboxTabNameAttribute("AQB3: Metadata Providers")]
	public class OLEDBMetadataProvider : BaseMetadataProvider
	{
		private OleDbConnection _connection;


		[Browsable(true)]
		public override IDbConnection Connection { get { return _connection; } set { SetConnection(value); } }


		static OLEDBMetadataProvider()
		{
			Helpers.MetadataProviderList.RegisterMetadataProvider(typeof(OLEDBMetadataProvider));
		}

		public OLEDBMetadataProvider()
		{
		}

		public OLEDBMetadataProvider(IContainer container) : this()
		{
			container.Add(this);
		}

		private void SetConnection(IDbConnection value)
		{
			if (_connection != value)
			{
				_connection = (OleDbConnection) value;
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
			try
			{
				if (!Connected) Connect();

				OleDbCommand command = _connection.CreateCommand();
				command.CommandTimeout = CommandTimeout;
				command.CommandText = sql;

				if (schemaOnly)
				{
					try
					{
						return command.ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo);
					}
					catch
					{
						// try to execute command without SchemaOnly flag, some providers (Advantage) are buggy
						return command.ExecuteReader();
					}
				}

				return command.ExecuteReader();
			}
			catch (Exception e)
			{
                throw new QueryBuilderException(ErrorCode.ErrorExecutingQuery,
                    e.Message + "\n\n" + Helpers.Localizer.GetString("strQuery", Constants.strQuery) + "\n" + sql);
			}
		}

		protected override void ExecSQLInternal(string sql)
		{
			try
			{
                if (!Connected) Connect();

				OleDbCommand command = _connection.CreateCommand();
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
			if (!Connected) Connect();

			// load from OLEDB catalogs
			try
			{
				DataTable schemaTable = _connection.GetOleDbSchemaTable(OleDbSchemaGuid.Catalogs, null);
				using (MetadataNamespacesFetcherFromDatatable mdf = 
					new MetadataNamespacesFetcherFromDatatable(databases,MetadataType.Database,loadingOptions))
				{
					mdf.Datatable = schemaTable;
					mdf.NameFieldName = "CATALOG_NAME";

					mdf.LoadMetadata();
				}
			}
			catch
			{
				// loading from OLEDB catalog failed
			}

			// load default database
			string currentDatabase = Connection.Database;
			if (!string.IsNullOrEmpty(currentDatabase))
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

		public override void LoadSchemas(MetadataList schemas, MetadataLoadingOptions loadingOptions)
		{
			if (!Connected) Connect();
			SQLContext sqlContext = schemas.SQLContext;

			string olddb = Connection.Database;

			// load from OLEDB catalog
			try
			{
				string[] restrictions = new string[1];

				if (sqlContext.SyntaxProvider.IsSupportDatabases())
				{
					MetadataNamespace database = schemas.Parent.Database;
					if (database != null)
						restrictions[0] = database.Name;
					else
						return;
				}
				else
				{
					restrictions[0] = null;
				}

				try
				{
					if (!string.IsNullOrEmpty(restrictions[0]) && Connection.Database != restrictions[0])
					{
						Connection.ChangeDatabase(restrictions[0]);
					}

					DataTable schemaTable = _connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, restrictions);
					DataColumn schemaColumn = schemaTable.Columns["TABLE_SCHEMA"];

					using (MetadataNamespacesFetcherFromStringList mnf = new MetadataNamespacesFetcherFromStringList(schemas, MetadataType.Schema, loadingOptions))
					{
						for (int i = 0; i < schemaTable.Rows.Count; i++)
						{
							DataRow currentRow = schemaTable.Rows[i];
							if (!currentRow.IsNull(schemaColumn))
							{
								string schemaName = Convert.ToString(currentRow[schemaColumn]);
								if (mnf.AllNamespaces.IndexOf(schemaName) == -1)
									mnf.AllNamespaces.Add(schemaName);
							}
						}

						mnf.LoadMetadata();
					}
				}
				catch
				{
					// loading from OLEDB catalog failed
				}
			}
			finally
			{
				try
				{
					if (Connection.Database != olddb)
						Connection.ChangeDatabase(olddb);
				}
				catch
				{
					// switch database back failed
				}
			}
		}

		public override void LoadObjects(MetadataList objects, MetadataLoadingOptions loadingOptions)
		{
			if (!Connected) Connect();
			SQLContext sqlContext = objects.SQLContext;

			string olddb = Connection.Database;

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

				try
				{
					if (!string.IsNullOrEmpty(restrictions[0]) && Connection.Database != restrictions[0])
					{
						Connection.ChangeDatabase(restrictions[0]);
					}

					DataTable schemaTable = _connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, restrictions);
					
					MetadataObjectFetcherFromDatatable mof = new MetadataObjectFetcherFromDatatable(objects, loadingOptions);
					mof.NameFieldName = "TABLE_NAME";
					mof.TypeFieldName = "TABLE_TYPE";
					mof.TableType = new[] { "TABLE", "LINK", "PASS-THROUGH", "SYNONYM" };
					mof.SystemTableType = new[] { "SYSTEM TABLE", "ACCESS TABLE" };
					mof.ViewType = "VIEW";
					mof.SystemViewType = "SYSTEM VIEW";
					mof.SynonymType = "ALIAS";
					mof.DescriptionFieldName = "DESCRIPTION";
					mof.Datatable = schemaTable;

					mof.LoadMetadata();
				}
				catch
				{
					// loading from OLEDB catalog failed
				}
			}
			finally
			{
				if (Connection.Database != olddb)
				{
					Connection.ChangeDatabase(olddb);
				}
			}
		}

		public override void LoadForeignKeys(MetadataList foreignKeys, MetadataLoadingOptions loadingOptions)
		{
			if (!Connected) Connect();

			MetadataItem obj = foreignKeys.Parent.Object;

			if (obj.Server == null)
			{
				MetadataItem schema = obj.Schema;
				MetadataItem database = obj.Database;

				try
				{
					object[] restrictions = new object[6];
					restrictions[3] = database != null ? database.Name : null;
					restrictions[4] = schema != null ? schema.Name : null;
					restrictions[5] = obj.Name;

					DataTable schemaTable = _connection.GetOleDbSchemaTable(OleDbSchemaGuid.Foreign_Keys, restrictions);

					MetadataForeignKeysFetcherFromDatatable mrf = new MetadataForeignKeysFetcherFromDatatable(foreignKeys,
					                                                                                          loadingOptions);

					mrf.PkDatabaseFieldName = "PK_TABLE_CATALOG";
					mrf.PkSchemaFieldName = "PK_TABLE_SCHEMA";
					mrf.PkNameFieldName = "PK_TABLE_NAME";
					mrf.PkFieldName = "PK_COLUMN_NAME";
					mrf.FkFieldName = "FK_COLUMN_NAME";
					mrf.OrdinalFieldName = "ORDINAL";
					mrf.Datatable = schemaTable;

					mrf.LoadMetadata();
				}
				catch (Exception exception)
				{
					throw new QueryBuilderException(exception.Message, exception);
				}
			}
		}

		private void LoadFieldsFromGetSchema(MetadataList fields, MetadataLoadingOptions loadingOptions)
		{
			SQLContext sqlContext = fields.SQLContext;

			string olddb = Connection.Database;

			try
			{
				// load tables
				string[] restrictions = new string[3];

				if (sqlContext.SyntaxProvider.IsSupportDatabases())
				{
					MetadataNamespace database = fields.Parent.Database;
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
					MetadataNamespace schema = fields.Parent.Schema;
					if (schema != null)
						restrictions[1] = schema.Name;
					else
						return;
				}
				else
				{
					restrictions[1] = null;
				}

				MetadataObject obj = fields.Parent.Object;
				if (obj == null) return;

				restrictions[2] = obj.Name;

				try
				{
					if (!string.IsNullOrEmpty(restrictions[0]) && Connection.Database != restrictions[0])
					{
						Connection.ChangeDatabase(restrictions[0]);
					}

					DataTable schemaTable = _connection.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, restrictions);
					schemaTable.DefaultView.Sort = "ORDINAL_POSITION";
					DataTable ordinalSortedColumns = schemaTable.DefaultView.ToTable();
                    
					MetadataFieldsFetcherFromDatatable mof = new MetadataFieldsFetcherFromDatatable(sqlContext);
					mof.NameFieldName = "COLUMN_NAME";
					mof.SizeFieldName = "CHARACTER_MAXIMUM_LENGTH";
					mof.PrecisionFieldName = "NUMERIC_PRECISION";
					mof.ScaleFieldName = "NUMERIC_SCALE";
					mof.DescriptionFieldName = "DESCRIPTION";
					mof.Datatable = ordinalSortedColumns;

					mof.LoadFields(fields);
				}
				catch
				{
					// loading from OLEDB catalog failed
				}
			}
			finally
			{
				if (Connection.Database != olddb)
				{
					Connection.ChangeDatabase(olddb);
				}
			}
		}

		public override void LoadFields(MetadataList fields, MetadataLoadingOptions loadingOptions)
		{
			base.LoadFields(fields, loadingOptions);

			LoadFieldsFromGetSchema(fields, loadingOptions);
		}

		public override string GetMetadataProviderDescription()
		{
			return "OLE DB Metadata Provider";
		}

		public override bool CanCreateInternalConnection()
		{
			return true;
		}

		public override void CreateAndBindInternalConnectionObj()
		{
			base.CreateAndBindInternalConnectionObj();
			Connection = new OleDbConnection();
			AddInternalConnectionObject(Connection);
		}
	}
}
