using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.Data.SqlClient;

namespace FirstClrFunctionProject
{
    public partial class TheUserDefinedFunctions
    {
        private class Person
        {
            public SqlInt32 PersonId;
            public SqlString PersonName;

            public Person(SqlInt32 personId, SqlString personName)
            {
                PersonId = personId;
                PersonName = personName;
            }
        }


        [SqlFunction(
           DataAccess = DataAccessKind.Read,
           FillRowMethodName = "FindBandMembers_FillRow",
           TableDefinition = "PersonId int, PersonName nvarchar(4000)")]
        public static IEnumerable FindBandMembers(SqlString s)
        {
            bool tryTheAwesome = true;

            string field = s.Value;

            if (tryTheAwesome)
            {
                string connectionString; // "context connection=true"; // can't work on streaming, i.e. this connection string doesn't work if we are using: yield return
                connectionString = "data source=localhost;initial catalog=AdventureWorks2012;integrated security=SSPI;enlist=false";

                using (var connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (var personsFromDb = new SqlCommand("select BusinessEntityId, " +  field + " from Person.Person", connection))
                    using (var personsReader = personsFromDb.ExecuteReader())
                    {
                        while (personsReader.Read())
                        {
                            yield return new Person(personId: personsReader.GetInt32(0), personName: personsReader.GetString(1));
                        }
                    }//using                    
                }//using
            }//if

            yield return new Person(personId: 1, personName: "John");
            yield return new Person(personId: 2, personName: "Paul");
            yield return new Person(personId: 3, personName: "George");
            yield return new Person(personId: 4, personName: "Ringo");
            yield return new Person(personId: 5, personName: "Francisco");
            yield return new Person(personId: 6, personName: "Nino");
            yield return new Person(personId: 7, personName: "Marc");
            yield return new Person(personId: 8, personName: "Michael");


        }

        public static void FindBandMembers_FillRow(object personObj, out SqlInt32 personId, out SqlString personName)
        {
            var p = (Person)personObj;

            personId = p.PersonId;
            personName = p.PersonName;
        }
    }
}


/*

use master;

go


sp_configure 'show advanced options', 1;
GO
RECONFIGURE;
GO
sp_configure 'clr enabled', 1;
GO
RECONFIGURE;
GO


IF EXISTS (SELECT * from sys.asymmetric_keys where name = 'MyDllKey') begin
	drop LOGIN MyDllLogin;
	drop ASYMMETRIC KEY MyDllKey;
end;

go



IF NOT EXISTS (SELECT * from sys.asymmetric_keys where name = 'MyDllKey') begin
	CREATE ASYMMETRIC KEY MyDllKey FROM EXECUTABLE FILE = 'c:\x\FirstClrFunctionProject.dll';
	-- http://stackoverflow.com/questions/7503603/cannot-find-the-asymmetric-key-because-it-does-not-exist-or-you-do-not-have-p
	CREATE LOGIN MyDllLogin FROM ASYMMETRIC KEY MyDllKey;
	GRANT EXTERNAL ACCESS ASSEMBLY TO MyDllLogin;
end;

go



use AdventureWorks2012;


IF EXISTS (SELECT name FROM sysobjects WHERE name = 'FindBandMembers')
   DROP FUNCTION FindBandMembers;
go

IF EXISTS (SELECT name FROM sys.assemblies WHERE name = 'FirstClrFunctionProject')
   DROP ASSEMBLY FirstClrFunctionProject;
go





CREATE ASSEMBLY FirstClrFunctionProject FROM 'c:\x\FirstClrFunctionProject.dll'
WITH PERMISSION_SET = EXTERNAL_ACCESS;
GO




-- Thanks Stackoverflow! 
-- http://stackoverflow.com/questions/7823488/sql-server-could-not-find-type-in-the-assembly

CREATE FUNCTION FindBandMembers(@hmm nvarchar(4000)) 
RETURNS TABLE (
   PersonId int,
   PersonName nvarchar(4000)
)
AS EXTERNAL NAME FirstClrFunctionProject.[FirstClrFunctionProject.TheUserDefinedFunctions].[FindBandMembers];
go

SELECT * FROM dbo.FindBandMembers('FirstName') order by personId;
go


-- http://stackoverflow.com/questions/6901811/sql-clr-streaming-table-valued-function-results

*/