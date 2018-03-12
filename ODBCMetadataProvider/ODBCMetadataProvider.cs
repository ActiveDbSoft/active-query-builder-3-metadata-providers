using System;
using System.Data;
using System.Data.Odbc;
using System.ComponentModel;

namespace ActiveQueryBuilder.Core
{
    /// <summary> Metadata Provider for ODBC Data Sources. </summary>
    [ToolboxItem(true)]
	//[ToolboxBitmap(typeof(ODBCMetadataProvider))]
	[ToolboxTabNameAttribute("AQB3: Metadata Providers")]
	public class ODBCMetadataProvider : BaseMetadataProvider
	{
		private OdbcConnection _connection;


		[Browsable(true)]
		public override IDbConnection Connection { get { return _connection; } set { SetConnection(value); } }


		static ODBCMetadataProvider()
		{
			Helpers.MetadataProviderList.RegisterMetadataProvider(typeof(ODBCMetadataProvider));
		}

		public ODBCMetadataProvider()
		{
		}

		public ODBCMetadataProvider(IContainer container) : this()
		{
			container.Add(this);
		}

		private void SetConnection(IDbConnection value)
		{
			if (_connection != value)
			{
				_connection = (OdbcConnection) value;
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
			OdbcCommand command;
			OdbcDataReader reader;

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
			OdbcCommand command;

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
			if (!Connected) Connect();
			try
			{
				MetadataNamespacesFetcherFromDatatable mnf = new MetadataNamespacesFetcherFromDatatable(databases, MetadataType.Database, loadingOptions);
				mnf.NameFieldName = "TABLE_CAT";

				using (DataTable schemaTable = _connection.GetSchema("Tables"))
				{
					// pre-ODBC v3
					if (schemaTable.Columns.IndexOf("TABLE_CAT") == -1)
					{
						// ODBC v2
						if (schemaTable.Columns.IndexOf("TABLE_QUALIFIER") != -1)
							mnf.NameFieldName = "TABLE_QUALIFIER";
						// ODBC v1?
						else if (schemaTable.Columns.IndexOf("QUALIFIERNAME") != -1)
							mnf.NameFieldName = "QUALIFIERNAME";
					}

					mnf.Datatable = schemaTable;
					mnf.LoadMetadata();
				}
			}
			catch (Exception exception)
			{
				throw new QueryBuilderException(exception.Message, exception);
			}
		}

		public override void LoadSchemas(MetadataList schemas, MetadataLoadingOptions loadingOptions)
		{
			if (!Connected) Connect();
			SQLContext sqlContext = schemas.SQLContext;

			string olddb = Connection.Database;

			try
			{
				try
				{
					// load tables
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

					DataTable schemaTable = _connection.GetSchema("Tables", restrictions);
					DataColumn schemaColumn = (schemaTable.Columns["TABLE_SCHEM"] ?? 
						schemaTable.Columns["TABLE_OWNER"]) ?? 
						schemaTable.Columns["TABLEOWNER"] ??
						schemaTable.Columns["OWNERNAME"];
					if (schemaColumn == null) return;

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

                        if (mnf.AllNamespaces.Count > 1 ||
                            mnf.AllNamespaces.Count == 1 && !string.IsNullOrEmpty(mnf.AllNamespaces[0]))
                            mnf.LoadMetadata();
					}
				}
				catch (Exception exception)
				{
					throw new QueryBuilderException(exception.Message, exception);
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

		public override void LoadObjects(MetadataList objects, MetadataLoadingOptions loadingOptions)
		{
			if (!Connected) Connect();
			SQLContext sqlContext = objects.SQLContext;

			string olddb = Connection.Database;

			try
			{
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

					MetadataObjectFetcherFromDatatable mof = new MetadataObjectFetcherFromDatatable(objects, loadingOptions);
					mof.NameFieldName = "TABLE_NAME";
					mof.TypeFieldName = "TABLE_TYPE";
					mof.TableType = new string[] { "TABLE", "LINK", "PASS-THROUGH", "SYNONYM" };
					mof.SystemTableType = new string[] { "SYSTEM TABLE", "ACCESS TABLE" };
					mof.ViewType = new string[] { "VIEW" };
					mof.SystemViewType = "SYSTEM VIEW";

					using (DataTable schemaTable = _connection.GetSchema("Tables", restrictions))
					{
						// hack for old outdated ODBC drivers which uses TABLENAME instead TABLE_NAME
						if (schemaTable.Columns.IndexOf("TABLENAME") != -1)
							mof.NameFieldName = "TABLENAME";

						// hack for old outdated ODBC drivers which uses TABLETYPE instead TABLE_TYPE
						if (schemaTable.Columns.IndexOf("TABLETYPE") != -1)
							mof.NameFieldName = "TABLETYPE";

						mof.DefaultObjectType = MetadataType.Table;
						mof.Datatable = schemaTable;
						mof.LoadMetadata();
					}

					using (DataTable schemaTable = _connection.GetSchema("Views", restrictions))
					{
						// hack for old outdated ODBC drivers which uses TABLENAME instead TABLE_NAME
						if (schemaTable.Columns.IndexOf("TABLENAME") != -1)
							mof.NameFieldName = "TABLENAME";

						// hack for old outdated ODBC drivers which uses TABLETYPE instead TABLE_TYPE
						if (schemaTable.Columns.IndexOf("TABLETYPE") != -1)
							mof.NameFieldName = "TABLETYPE";

						mof.DefaultObjectType = MetadataType.View;
						mof.Datatable = schemaTable;
						mof.LoadMetadata();
					}
				}
				catch (Exception exception)
				{
					throw new QueryBuilderException(exception.Message, exception);
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

                    DataTable schemaTable = _connection.GetSchema("Columns", restrictions);
                    schemaTable.DefaultView.Sort = "ORDINAL_POSITION";
                    DataTable ordinalSortedColumns = schemaTable.DefaultView.ToTable();

                    MetadataFieldsFetcherFromDatatable mof = new MetadataFieldsFetcherFromDatatable(sqlContext);
                    mof.NameFieldName = "COLUMN_NAME";
                    mof.ServerTypeFieldName = "TYPE_NAME";
                    mof.SizeFieldName = "COLUMN_SIZE";
                    mof.ScaleFieldName = "DECIMAL_DIGITS";
                    mof.DescriptionFieldName = "REMARKS";
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
			return "ODBC Metadata Provider";
		}

		public override bool CanCreateInternalConnection()
		{
			return true;
		}

	    public override void CreateAndBindInternalConnectionObj()
		{
			base.CreateAndBindInternalConnectionObj();
			Connection = new OdbcConnection();
			AddInternalConnectionObject(Connection);
		}
	}
}
