USE [master]
GO

IF NOT EXISTS (SELECT * FROM sys.sql_logins WHERE name = 'superset_user')
BEGIN
    CREATE LOGIN [superset_user] WITH PASSWORD = '123', 
    CHECK_POLICY = OFF, 
    CHECK_EXPIRATION = OFF
    PRINT 'Login superset_user created'
END
GO

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'SupersetMetaDB')
BEGIN
    CREATE DATABASE [SupersetMetaDB]
    PRINT 'Database SupersetMetaDB created'
END
GO

USE [SupersetMetaDB]
GO

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'superset_user')
BEGIN
    CREATE USER [superset_user] FOR LOGIN [superset_user]
    ALTER ROLE [db_owner] ADD MEMBER [superset_user]
    PRINT 'User superset_user added as db_owner'
END
GO