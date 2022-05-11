using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;


namespace YardiToBlacklineFunction
{
    public class Function1
    {
        [FunctionName ( "Function1" )]
        public async Task RunAsync ( [TimerTrigger ( "0 */1 * * * *" )] TimerInfo myTimer, ILogger log )
        {
            // [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequestMessage req,
            log.LogInformation ( $"C# Timer trigger function executed at: {DateTime.Now}" );
            //call Blackline and get the token           
            Token token = new Token ();
            var tokenurl = Environment.GetEnvironmentVariable ( "blacklineUrlBase" ) + "connect/token";
            HttpClient newClient = new HttpClient ();

            var data = new[]
                        {
                            new KeyValuePair<string, string>("grant_type", "password"),
                            new KeyValuePair<string, string>("password", "f687e84fa6ee5856203bccda73262fe2"),
                            new KeyValuePair<string, string>("username", "onecloud"),
                            new KeyValuePair<string, string>("scope", "DataIngestionAPI instance_6E92F34B-C06F-49ED-AF71-CA906EA1755B"),
                            new KeyValuePair<string, string>("client_secret", "&[pro~)=tn4e"),
                            new KeyValuePair<string, string>("client_id", "linklogistics_sts2")
                        };
            //Read Server Response
            newClient.DefaultRequestHeaders.TryAddWithoutValidation ( "Content-Type", "application/octet-stream" );
            HttpResponseMessage tokenresponse = await newClient
                .PostAsync ( string.Format ( tokenurl ), new FormUrlEncodedContent ( data ) );
            if (tokenresponse.StatusCode == HttpStatusCode.OK)
            {
                var jsonContent = await tokenresponse.Content.ReadAsStringAsync ();
                token = JsonConvert.DeserializeObject<Token> ( jsonContent );

            }
            else
            {
                log.LogInformation ( $"Function is unable to get Blackline token at: {DateTime.Now}" );
                return;
            }
            //Call yardi get data
            // Get the connection string from app settings and use it to create a connection.
            var str = Environment.GetEnvironmentVariable ( "IntegrationDBsql_connection" );
            DataTable dt = new DataTable ();
            using (SqlConnection conn = new SqlConnection ( str ))
            {
                conn.Open ();
                //check batch
                var batchsql = "SELECT * FROM Batching WHERE  [LastrunDateTime] = (SELECT MAX([LastrunDateTime]) FROM Batching) ";
                int startcount = 0;
                int endcount = 100;
                using (SqlCommand cmd = new SqlCommand ( batchsql, conn ))
                {
                    SqlDataReader dr = cmd.ExecuteReader ();
                    while (dr.Read ())
                    {
                        startcount = int.Parse(dr[2].ToString ());
                        endcount = startcount + 100;
                    }
                    dr.Close ();
                }
                //update batch
                //INSERT INTO[dbo].[Batching] ([LastrunDateTime],[StartCount],[Endcount]) VALUES
                using (SqlCommand command = conn.CreateCommand ())
                {
                    command.CommandText = "INSERT INTO Batching (LastrunDateTime, StartCount, Endcount)  VALUES ( @ln, @fn, @add )";

   command.Parameters.AddWithValue ( "@ln", DateTime.Now );
                    command.Parameters.AddWithValue ( "@fn", startcount );
                    command.Parameters.AddWithValue ( "@add", endcount );

                    command.ExecuteNonQuery ();
                }

                //get data
                var text = "  declare @selectPeriodDate Date = '2022-03-31' " +
" select 'PRO-C' + char(9) + AccountNumber + char(9) + PropertyID + char(9) " +
"                               + Region + char(9) + Department + char(9)  " +
"							   + Acquisition_Portfolio + char(9) + Sub_Region + char(9) + Cash_Entity + char(9) + Tenant_Code + char(9) + isnull(Book,'') + char(9) + AccountDescription  " +
"							   + char(9) + 'cash' + char(9) + FinancialStatement	 + char(9) + 'Asset'  " +
"                            + char(9) + ActiveAccount + char(9) + ActivityInPeriod + char(9) + 'GBP' + char(9) + AccountCurrency + char(9) + CONVERT(nvarchar,@selectPeriodDate) + char(9) +  " +
"							GLAccountBalance + char(9) + '0' + char(9) + GLAccountBalance " +
"		 + char(9) + 'IMPCash 2' + char(9) + 'IMPCash 3' + char(9) + 'IMPCash 4' + char(9) + 'IMPCash 5' + char(9) + 'IMPCash 6'  " +
"         AS datarow, " +
"		 CONVERT(varchar,AccountNumber)+ '|' + CONVERT(varchar,PropertyID) +'|'  " +
"		 + CONVERT(varchar,@selectPeriodDate) +'|' + ISNULL(CONVERT(varchar,Book),0)  AS logid, " +
"		 GLAccountBalance " +
" from [dbo].[AccountsGL] where id>" + startcount + " and id<=" + endcount;

                using (SqlCommand cmd = new SqlCommand ( text, conn ))
                {
                    SqlDataReader dr = cmd.ExecuteReader ();
                    while (dr.Read ())
                    {
                        //log.LogInformation ( $" Function successfully read data: ", dr[0] );
                        //call blackline http and send data
                        try
                        {
                            var url = Environment.GetEnvironmentVariable ( "blacklineUrlBase" ) + "dataingestion/accounts?message-version=5";
                            // Convert all request param into Json object

                            var content = dr[0].ToString ();
                            //"PRO-C	36010000	42	1102	BK	12111				Current Year Retained Earnings	Cash	A	         0	TRUE	TRUE	GBP	USD	3/31/2022	34664.86	23058.37	34664.86	IMPCash 2	IMPCash 3	IMPCash 4	IMPCash 5	IMPCash 6";

                            // Call Your  API
                            HttpRequestMessage newRequest = new HttpRequestMessage ( HttpMethod.Put, string.Format ( url ) );
                            newRequest.Headers.Add ( "Authorization", "Bearer " +
                                token.AccessToken );
                            newRequest.Content = new StringContent ( content, Encoding.UTF8, "application/json" );
                            //Read Server Response
                            HttpResponseMessage response = await newClient.SendAsync ( newRequest );
                            //if (response.StatusCode == HttpStatusCode.Accepted)
                            //    log.LogInformation ( $" Function successfully inserterted: ", response.Content );
                            //else
                            //    log.LogInformation ( $" Function Failed in the Blackline API call: ", response.Content );

                        }
                        catch (Exception ex)
                        {
                            log.LogInformation ( $" Exception occured in function executed at: {DateTime.Now} ", ex.Message );
                            //Log it to a table for reporting
                            //Incremental load
                            //Alerts and notification
                        }
                        finally
                        {

                        }
                    }
                }
            }

            log.LogInformation ( $"C# Timer trigger function Ent at: {DateTime.Now}" );
            //------------------------

        }


    }

    internal class Token
    {
        [JsonProperty ( "access_token" )]
        public string AccessToken { get; set; }

        [JsonProperty ( "token_type" )]
        public string TokenType { get; set; }

        [JsonProperty ( "expires_in" )]
        public int ExpiresIn { get; set; }

        [JsonProperty ( "refresh_token" )]
        public string RefreshToken { get; set; }
    }


}
