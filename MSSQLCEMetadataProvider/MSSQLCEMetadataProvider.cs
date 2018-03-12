using System;
using System.ComponentModel;
using System.Data;
using System.Data.SqlServerCe;

namespace ActiveQueryBuilder.Core
{
	/// <summary> Metadata Provider for Microsoft SQL Server Mobile. </summary>
	[ToolboxItem(true)]
	//[ToolboxBitmap(typeof(MSSQLCEMetadataProvider))]
	[ToolboxTabNameAttribute("AQB3: Metadata Providers")]
	public class MSSQLCEMetadataProvider : BaseMetadataProvider
	{
		private SqlCeConnection _connection = null;
		private SQLQualifiedName _defaultDatabaseName = null;

		[Browsable(true)]
		public override IDbConnection Connection { get { return _connection; } set { SetConnection(value); } }

		static MSSQLCEMetadataProvider()
		{
			Helpers.MetadataProviderList.RegisterMetadataProvider(typeof(MSSQLCEMetadataProvider));
		}

		public MSSQLCEMetadataProvider()
		{
		}

		public MSSQLCEMetadataProvider(IContainer container) : this()
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
				_connection = (SqlCeConnection) value;
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
			SqlCeCommand command;
			SqlCeDataReader reader;

			try
			{
				if (!Connected)
				{
					Connect();
				}

				command = (SqlCeCommand) Connection.CreateCommand();
				//command.CommandTimeout = CommandTimeout; // MSSQL CE does not support Command Timeout!
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
			SqlCeCommand command;

			try
			{
				if (!Connected) Connect();

				command = (SqlCeCommand) Connection.CreateCommand();
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
			return "MSSQL CE Metadata Provider";
		}

		public override bool CanCreateInternalConnection()
		{
			return true;
		}

		public override void CreateAndBindInternalConnectionObj()
		{
			base.CreateAndBindInternalConnectionObj();

			Connection = new SqlCeConnection();

			AddInternalConnectionObject(Connection);
		}
	}
}
