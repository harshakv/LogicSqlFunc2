using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Data.SqlClient;
using System.Data;

namespace LogicSqlFunc2
{
    public static class LogicSqlFunc2
    {
        [FunctionName("LogicSqlFunc2")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string name = req.Query["name"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            string data = requestBody;//JsonConvert.DeserializeObject(requestBody);
            string input = data; //"ID,Name,Salary 1,John,1000  2,Shankar,2000  3,David,5000  4,Leuton,4000  5,Sam,3000";
            //name = name ?? data?.name;

            //string responseMessage = string.IsNullOrEmpty(name)
            //    ? "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
            //    : $"Hello, {name}. This HTTP triggered function executed successfully.";



            string[] rows = input.Replace("\r\n", " ").Split(" ");
            int inseretedRows = default;
            string result = string.Empty;

            

            var str = "Server=tcp:ssispocserver.database.windows.net,1433;Initial Catalog=ssissqldatabase;Persist Security Info=False;User ID=admin123;Password=password1$;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
            using (SqlConnection conn = new SqlConnection(str))
            {
                conn.Open();

                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "INSERT INTO Employee(ID, [Name], Salary) VALUES(@ID, @Name, @Salary);";

                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.Parameters.Add(new SqlParameter("@ID", SqlDbType.Int));
                    cmd.Parameters.Add(new SqlParameter("@Name", SqlDbType.VarChar));
                    cmd.Parameters.Add(new SqlParameter("@Salary", SqlDbType.Int));

                    foreach (var item in rows)
                    {
                        try
                        {
                            var rowParts = item.Split(","); ;

                            cmd.Parameters["@ID"].Value = nulltoint(rowParts[0]);
                            cmd.Parameters["@Name"].Value = nulltostring(rowParts[1]);
                            cmd.Parameters["@Salary"].Value = nulltoint(rowParts[2]);

                            // Execute the command and log the # rows affected.
                            inseretedRows = await cmd.ExecuteNonQueryAsync();
                            log.LogInformation(rowParts[1] + " row added");

                            if (inseretedRows != 1)
                            {
                                log.LogError(String.Format("Row for employee {0} was not added to the database", rowParts[1]));
                            }
                            result = result + rowParts[1].ToString() + " , ";
                        }
                        catch (Exception ex)
                        {
                            result = result + ex.Message;
                        }
                    }

                }
            }
            return new OkObjectResult("Its Done! Inserted names: " + result);
        }
        private static string nulltostring(object Value)
        {
            return Value == null ? "" : Value.ToString();
        }

        private static int nulltoint(object Value)
        {
            return Value == null ? 0 : Convert.ToInt32(Value);
        }
    }
}
