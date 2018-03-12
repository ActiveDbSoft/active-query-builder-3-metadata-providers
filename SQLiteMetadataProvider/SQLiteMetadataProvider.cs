using System;
using System.Data;
using System.ComponentModel;
using System.Data.SQLite;

namespace ActiveQueryBuilder.Core
{
    /// <summary>Metadata Provider for generic .NET data provider (IDbConnection interface).</summary>
	[ToolboxItem(true)]
	[ToolboxTabNameAttribute("AQB3: Metadata Providers")]
	public class SQLiteMetadataProvider : BaseMetadataProvider
	{
		private SQLiteConnection _connection;

        /// <summary>Refers to the object that implements the IDbConnection interface.</summary>
		[Browsable(true)]
		public override IDbConnection Connection { get { return _connection; } set { SetConnection(value); } }

		static SQLiteMetadataProvider()
		{
			Helpers.MetadataProviderList.RegisterMetadataProvider(typeof(SQLiteMetadataProvider));
		}

		public SQLiteMetadataProvider()
		{
		}

		public SQLiteMetadataProvider(IContainer container) : this()
		{
			container.Add(this);
		}

		private void SetConnection(IDbConnection value)
		{
			if (_connection != value)
			{
				_connection = (SQLiteConnection) value;
			}
		}

	    protected override bool IsConnected()
	    {
	        return Connection != null && (Connection.State & ConnectionState.Open) == ConnectionState.Open;
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
                throw new QueryBuilderException(ErrorCode.ErrorNoConnectionObject, string.Format(Constants.strNoConnectionObject, "Connection"));
			}
		}

		protected override IDataReader PrepareSQLDatasetInternal(string sql, bool schemaOnly)
		{
		    SQLiteDataReader reader;
            try
			{
			    if(!Connected)
			    {
			        Connect();
			    }
			    SQLiteCommand command = _connection.CreateCommand();
				command.CommandTimeout = CommandTimeout;
				command.CommandText = sql;
				reader = schemaOnly 
                    ? command.ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo) 
                    : command.ExecuteReader();
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
		    try
			{
			    if(!Connected)
			    {
			        Connect();
			    }
			    SQLiteCommand command = _connection.CreateCommand();
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
		    if(!Connected)
		    {
		        Connect();
		    }
		    // load from OLEDB catalogs
			try
			{
				DataTable schemaTable = _connection.GetSchema("Tables");
				string databaseFieldName = "TABLE_CATALOG";
				if (schemaTable.Columns[databaseFieldName] == null) 
					databaseFieldName = "TABLE_CAT";

				using (MetadataNamespacesFetcherFromDatatable mdf =
					new MetadataNamespacesFetcherFromDatatable(databases, MetadataType.Database, loadingOptions))
				{
					mdf.Datatable = schemaTable;
					mdf.NameFieldName = databaseFieldName;

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
				    database = new MetadataNamespace(databases, MetadataType.Database)
				    {
				        Name = currentDatabase
				    };
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
				string[] restrictions = new string[4];

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
					DataTable schemaTable = _connection.GetSchema("Tables", restrictions);
					DataColumn schemaColumn = schemaTable.Columns["TABLE_SCHEMA"] ?? schemaTable.Columns["TABLE_SCHEM"];
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
				string[] restrictions = new string[4];
				if (sqlContext.SyntaxProvider.IsSupportDatabases())
				{
					MetadataNamespace database = objects.Parent.Database;
				    if(database != null)
				    {
				        restrictions[0] = database.Name;
				    } else
				    {
				        return;
				    }
				}
				else
				{
					restrictions[0] = null;
				}
				if (sqlContext.SyntaxProvider.IsSupportSchemas())
				{
					MetadataNamespace schema = objects.Parent.Schema;
				    if(schema != null)
				    {
				        restrictions[1] = schema.Name;
				    } else
				    {
				        return;
				    }
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
					DataTable schemaTable = _connection.GetSchema("Tables", restrictions);
				    MetadataObjectFetcherFromDatatable mof = new MetadataObjectFetcherFromDatatable(objects, loadingOptions)
				    {
				        NameFieldName = "TABLE_NAME",
				        TypeFieldName = "TABLE_TYPE",
				        TableType = new[]
				        {
				            "TABLE",
				            "LINK",
				            "PASS-THROUGH",
				            "SYNONYM",
				            "BASE TABLE"
				        },
				        SystemTableType = new[]
				        {
				            "SYSTEM TABLE",
				            "ACCESS TABLE"
				        },
				        ViewType = "VIEW",
				        SystemViewType = "SYSTEM VIEW",
				        SynonymType = "ALIAS",
				        Datatable = schemaTable
				    };
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

		public override string GetMetadataProviderDescription()
		{
			return "SQLite Metadata Provider";
		}

		public override bool CanCreateInternalConnection()
		{
			return false;
		}

		public override void LoadForeignKeys(MetadataList foreignKeys, MetadataLoadingOptions loadingOptions)
		{
            MetadataItem obj = foreignKeys.Parent.Object;
            if (obj.Server == null)
            {
                MetadataItem database = obj.Database;
                try
                {
                    string[] restrictions = new string[5];
                    restrictions[0] = database != null ? database.Name : null;
                    restrictions[2] = obj.Name;
                    DataTable schemaTable = _connection.GetSchema("ForeignKeys", restrictions);
                    MetadataForeignKeysFetcherFromDatatable mrf = new MetadataForeignKeysFetcherFromDatatable(foreignKeys, loadingOptions)
                    {
                        PkDatabaseFieldName = "FKEY_TO_CATALOG",
                        PkNameFieldName = "FKEY_TO_TABLE",
                        PkFieldName = "FKEY_TO_COLUMN",
                        FkFieldName = "FKEY_FROM_COLUMN",
                        OrdinalFieldName = "FKEY_FROM_ORDINAL_POSITION",
                        Datatable = schemaTable
                    };
                    mrf.LoadMetadata();
                }
                catch (Exception exception)
                {
                    throw new QueryBuilderException(exception.Message, exception);
                }
            }
        }

		public override bool GetReferencingObjectNames(UpdatableObjectList<MetadataQualifiedName> list, MetadataObject forObject, MetadataLoadingOptions loadingOptions)
		{
			if (forObject.Server == null)
			{
				MetadataNamespace database = forObject.Database;
				MetadataNamespace schema = forObject.Schema;
				string[] restrictions = new string[6];
				restrictions[0] = database != null ? database.Name : null;
				restrictions[1] = schema != null ? schema.Name : null;
				restrictions[2] = forObject.Name;
				using (DataTable foreignKeys = _connection.GetSchema("ForeignKeys", restrictions))
				{
					int databaseField = foreignKeys.Columns.IndexOf("FKTABLE_CAT");
					int schemaField = foreignKeys.Columns.IndexOf("FKTABLE_SCHEM");
					int nameField = foreignKeys.Columns.IndexOf("FKTABLE_NAME");
					foreach (DataRow row in foreignKeys.Rows)
					{
						object referencingName = nameField != -1 ? row[nameField] : null;
						object referencingSchema = schemaField != -1 ? row[schemaField] : null;
						object referencingDatabase = databaseField != -1 ? row[databaseField] : null;
						MetadataQualifiedName name = new MetadataQualifiedName();
						if (referencingName != null) name.Add(referencingName.ToString(), MetadataType.Objects);
						if (referencingSchema != null) name.Add(referencingSchema.ToString(), MetadataType.Schema);
						if (referencingDatabase != null) name.Add(referencingDatabase.ToString(), MetadataType.Schema);
						list.Add(name);
					}
					return true;
				}
			}
			return false;
		}
	}
}
