using System;
using System.Data;
using System.ComponentModel;
using Npgsql;

namespace ActiveQueryBuilder.Core
{
    /// <summary> Metadata Provider for PostgreSQL databases. </summary>
    [ToolboxItem(true)]
	//[ToolboxBitmap(typeof(PostgreSQLMetadataProvider))]
	[ToolboxTabNameAttribute("AQB3: Metadata Providers")]
	public class PostgreSQLMetadataProvider : BaseMetadataProvider
	{
		private NpgsqlConnection _connection;
		private SQLQualifiedName _defaultDatabaseName;


		[Browsable(true)]
		public override IDbConnection Connection { get { return _connection; } set { SetConnection(value); } }


		static PostgreSQLMetadataProvider()
		{
			Helpers.MetadataProviderList.RegisterMetadataProvider(typeof(PostgreSQLMetadataProvider));
		}

		public PostgreSQLMetadataProvider()
		{
		}

		public PostgreSQLMetadataProvider(IContainer container) : this()
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
				_connection = (NpgsqlConnection) value;
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
			NpgsqlCommand command;
			NpgsqlDataReader reader;

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
			NpgsqlCommand command;

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

				try
				{
					DataTable schemaTable = _connection.GetSchema("Databases");

					using (MetadataNamespacesFetcherFromDatatable mdf = new MetadataNamespacesFetcherFromDatatable(databases, MetadataType.Database, loadingOptions))
					{
						mdf.Datatable = schemaTable;
						mdf.NameFieldName = "database_name";

						mdf.LoadMetadata();
					}
				}
				catch (Exception exception)
				{
					throw new QueryBuilderException(exception.Message, exception);
				}
			}
		}

		public override string GetMetadataProviderDescription()
		{
			return "PostgreSQL Metadata Provider";
		}

		public override bool CanCreateInternalConnection()
		{
			return true;
		}

	    public override void CreateAndBindInternalConnectionObj()
		{
			base.CreateAndBindInternalConnectionObj();
			Connection = new NpgsqlConnection();
			AddInternalConnectionObject(Connection);
		}
	}
}
