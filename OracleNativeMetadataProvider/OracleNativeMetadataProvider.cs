using System;
using System.Data;
using System.ComponentModel;
using Oracle.ManagedDataAccess.Client;

namespace ActiveQueryBuilder.Core
{
    /// <summary> Metadata Provider for Oracle Database through Oracle.ManagedDataAccess.Client (Oracle). </summary>
    /// <seealso cref="ActiveDatabaseSoftware.ActiveQueryBuilder.OracleMetadataProvider">Metadata Provider for Oracle Database through System.Data.OracleClient (Microsoft)</seealso>
    [ToolboxItem(true)]
	//[ToolboxBitmap(typeof(OracleNativeMetadataProvider))]
	[ToolboxTabNameAttribute("AQB3: Metadata Providers")]
	public class OracleNativeMetadataProvider : BaseMetadataProvider
	{
		private OracleConnection _connection;


		[Browsable(true)]
		public override IDbConnection Connection { get { return _connection; } set { SetConnection(value); } }


		static OracleNativeMetadataProvider()
		{
			Helpers.MetadataProviderList.RegisterMetadataProvider(typeof(OracleNativeMetadataProvider));
		}

		public OracleNativeMetadataProvider()
		{
		}

		public OracleNativeMetadataProvider(IContainer container) : this()
		{
			container.Add(this);
		}

		private void SetConnection(IDbConnection value)
		{
			if (_connection != value)
			{
				_connection = (OracleConnection) value;
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
			OracleCommand command;
			OracleDataReader reader;

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
			OracleCommand command;

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

		public override string GetMetadataProviderDescription()
		{
			return "Oracle Native Metadata Provider";
		}

		public override bool CanCreateInternalConnection()
		{
			return true;
		}

	    public override void CreateAndBindInternalConnectionObj()
		{
			base.CreateAndBindInternalConnectionObj();
			Connection = new OracleConnection();
			AddInternalConnectionObject(Connection);
		}
	}
}
