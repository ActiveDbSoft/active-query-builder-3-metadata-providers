using System;
using System.ComponentModel;
using System.Data;
using VistaDB.Provider;

namespace ActiveQueryBuilder.Core
{
    /// <summary> Metadata Provider for VistaDB 5. </summary>
    [ToolboxItem(true)]
	//[ToolboxBitmap(typeof(VistaDB5MetadataProvider))]
	[ToolboxTabNameAttribute("AQB3: Metadata Providers")]
	public class VistaDB5MetadataProvider : BaseMetadataProvider
	{
		private VistaDBConnection _connection;


		[Browsable(true)]
		public override IDbConnection Connection { get { return _connection; } set { SetConnection(value); } }


		static VistaDB5MetadataProvider()
		{
			Helpers.MetadataProviderList.RegisterMetadataProvider(typeof(VistaDB5MetadataProvider));
		}

		public VistaDB5MetadataProvider()
		{
		}

		public VistaDB5MetadataProvider(IContainer container) : this()
		{
			container.Add(this);
		}

		private void SetConnection(IDbConnection value)
		{
			if (_connection != value)
			{
				_connection = (VistaDBConnection) value;
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
			VistaDBCommand command;
			VistaDBDataReader reader;

			try
			{
				if (!Connected) Connect();

				command = (VistaDBCommand) Connection.CreateCommand();
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
			VistaDBCommand command;

			try
			{
				if (!Connected) Connect();

				command = (VistaDBCommand) Connection.CreateCommand();
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
			MetadataItem obj = objects.Parent;

			if (obj.Schema == null && obj.Database == null && obj.Server == null)
			{
				if (!Connected) Connect();

				try
				{
					// load tables
					DataTable schemaTable = _connection.GetSchema(VistaDBConnection.SchemaConstants.SCHEMA_TABLES);

					MetadataObjectFetcherFromDatatable mof = new MetadataObjectFetcherFromDatatable(objects, loadingOptions);
					mof.NameFieldName = "TABLE_NAME";
					mof.TypeFieldName = "TABLE_TYPE";
					mof.TableType = new string[] {VistaDBConnection.UserTableType};
					mof.SystemTableType = new string[] {VistaDBConnection.SystemTableType};
					mof.Datatable = schemaTable;

					mof.LoadMetadata();

					// load views
					schemaTable = _connection.GetSchema(VistaDBConnection.SchemaConstants.SCHEMA_VIEWS, null);

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
		}

		public override void LoadForeignKeys(MetadataList foreignKeys, MetadataLoadingOptions loadingOptions)
		{
			if (!Connected) Connect();

			// load FKs
			try
			{
				MetadataItem obj = foreignKeys.Parent.Object;

				string[] restrictions = new string[5];
				restrictions[2] = obj.Name;

				DataTable schemaTable = _connection.GetSchema(VistaDBConnection.SchemaConstants.SCHEMA_FOREIGNKEYCOLUMNS, restrictions);

				MetadataForeignKeysFetcherFromDatatable mrf = new MetadataForeignKeysFetcherFromDatatable(foreignKeys, loadingOptions);
				mrf.PkNameFieldName = "FKEY_TO_TABLE";
				mrf.PkFieldName = "FKEY_TO_COLUMN";
				mrf.FkFieldName = "FKEY_FROM_COLUMN";
				mrf.ForeignKeyIdFieldName = "FKEY_TO_TABLE";
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
			return "VistaDB5 Metadata Provider";
		}

		public override bool CanCreateInternalConnection()
		{
			return true;
		}

	    public override void CreateAndBindInternalConnectionObj()
		{
			base.CreateAndBindInternalConnectionObj();
			Connection = new VistaDBConnection();
			AddInternalConnectionObject(Connection);
		}
	}
}
