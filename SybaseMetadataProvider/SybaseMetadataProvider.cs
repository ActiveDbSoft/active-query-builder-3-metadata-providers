using System;
using System.Data;
using System.ComponentModel;
using iAnywhere.Data.SQLAnywhere;

namespace ActiveQueryBuilder.Core
{
    /// <summary> Metadata Provider for Sybase ASA. </summary>
    /// <remarks> Working with Sybase ASE, use the <see cref="UniversalMetadataProvider">Universal Metadata Provider</see>.</remarks>
    [ToolboxItem(true)]
	//[ToolboxBitmap(typeof(SybaseMetadataProvider))]
	[ToolboxTabNameAttribute("AQB3: Metadata Providers")]
	public class SybaseMetadataProvider : BaseMetadataProvider
	{
		private SAConnection _connection;
		private SQLQualifiedName _defaultDatabaseName;

		[Browsable(true)]
		public override IDbConnection Connection { get { return _connection; } set { SetConnection(value); } }

		static SybaseMetadataProvider()
		{
			Helpers.MetadataProviderList.RegisterMetadataProvider(typeof(SybaseMetadataProvider));
		}

		public SybaseMetadataProvider()
		{
		}

		public SybaseMetadataProvider(IContainer container) : this()
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
				_connection = (SAConnection) value;
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
			SACommand command;
			SADataReader reader;

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
			SACommand command;

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

		public override bool CanCreateInternalConnection()
		{
			return true;
		}

	    public override void CreateAndBindInternalConnectionObj()
		{
			base.CreateAndBindInternalConnectionObj();
			Connection = new SAConnection();
			AddInternalConnectionObject(Connection);
		}
	}
}
