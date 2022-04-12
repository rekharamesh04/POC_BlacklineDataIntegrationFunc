using System;
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

namespace YardiToBlacklineFunction
{
    public class Function1
    {
        [FunctionName("Function1")]
        public async Task RunAsync([TimerTrigger("0 */1 * * * *")] TimerInfo myTimer, ILogger log)
        {
            // [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequestMessage req,
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            //Call yardi get data
            // Get the connection string from app settings and use it to create a connection.
            var str = Environment.GetEnvironmentVariable("yardisql_connection");
            DataTable dt = new DataTable();
            using (SqlConnection conn = new SqlConnection(str))
            {
                conn.Open();
                var text = "select top 1 'PRO-C' + char(9) + RTRIM(a.SCODE) + char(9) + '' + RTRIM(p.HPROP) + char(9) + '1102' + char(9) + 'BK' + char(9) + '12111' + char(9) + '' + char(9) + '' + char(9) + '' + char(9) + a.SDESC + char(9) + b.BookName + char(9) + 'A' + char(9) + STR(a.IACCTTYPE) "+
    " + char(9) + 'TRUE' + char(9) + 'TRUE' + char(9) + 'GBP' + char(9) + 'USD' + char(9) + '6/30/2022' + char(9) + '34664.86' + char(9) + '23058.37' + char(9) + '34664.86' + char(9) + 'IMPCash 2' + char(9) + 'IMPCash 3' + char(9) + 'IMPCash 4' + char(9) + 'IMPCash 5' + char(9) + 'IMPCash 6' " +

"from(Select HPROP " +

      "from attributes " + 

      "where SUBGROUP58 in ('Acquired', 'Marketing for Sale', 'Pending Sale', 'Held for Sale', 'Sold') "+

        "AND SUBGROUP9 in ('Property', 'Development') "+

        "AND SPROPNAME not like '%Model of%' "+
    " ) p "+
"inner join(select top 100000 * from TOTAL) t "+
  " on t.hppty = p.HPROP "+
"inner join ACCT a "+

    "on t.hacct = a.hmy "+
"inner join UL_BOOKS b "+

    "on t.ibook = b.book";

                using (SqlCommand cmd = new SqlCommand(text, conn))
                {
                    SqlDataReader dr =cmd.ExecuteReader();
                   while(dr.Read())
                    {
                        log.LogInformation($" Function successfully read data: ", dr[0]);
                        //call blackline http and send data
                        try
                        {
                            var url = Environment.GetEnvironmentVariable("blacklineUrlBase") + "accounts?message-version=5";
                            // Convert all request param into Json object

                            var content = dr[0].ToString();
                                //"PRO-C	36010000	42	1102	BK	12111				Current Year Retained Earnings	Cash	A	         0	TRUE	TRUE	GBP	USD	3/31/2022	34664.86	23058.37	34664.86	IMPCash 2	IMPCash 3	IMPCash 4	IMPCash 5	IMPCash 6";

                            // Call Your  API
                            HttpClient newClient = new HttpClient();
                            HttpRequestMessage newRequest = new HttpRequestMessage(HttpMethod.Put, string.Format(url));
                            newRequest.Headers.Add("Authorization", "Bearer " +
                                "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkNINGwzQ0JqUDQ0R1QtSXJsVGNLbWxfc1hKTSIsImtpZCI6IkNINGwzQ0JqUDQ0R1QtSXJsVGNLbWxfc1hKTSJ9.eyJleHAiOjE2NDk3MDY5OTYsImlzcyI6Imh0dHBzOi8vc2J1cy5hcGkuYmxhY2tsaW5lLmNvbSIsImF1ZCI6Imh0dHBzOi8vc2J1cy5hcGkuYmxhY2tsaW5lLmNvbS9yZXNvdXJjZXMiLCJjbGllbnRfaWQiOiJsaW5rbG9naXN0aWNzX3N0czIiLCJzY29wZSI6WyJEYXRhSW5nZXN0aW9uQVBJIiwiaW5zdGFuY2VfNkU5MkYzNEItQzA2Ri00OUVELUFGNzEtQ0E5MDZFQTE3NTVCIl0sInN1YiI6IjMxNDA2NzhDLUQ5Q0ItNEFGMC05NUM2LUQ0NEZFRkREMkE4QSIsImF1dGhfdGltZSI6MTY0OTcwMzM5NiwiaWRwIjoiaWRzcnYiLCJsYW5ndWFnZSI6ImVuLVVTLGVuO3E9MC45In0.Ykrcsb3Qqp60XtjKrkQNbCDCY1W4PzpIs1bMEUlwk6CsoQKVwuQBQND3lRldsT58NgyRx3T_KpO2nFZ2cnk5cr_TU-Da3a3xep3CpyCrwYdIz_BcG7vDlhpW0nBRa8wSBWpNvNWcqChxp4saru5m6Jk2EEt1IlkELahXANir8ZLKxdqXz3Alk2yrrWBWtBvJUO8Y9Hf6YpOswvhOXnKhbtzi7aEtGdgG9A_V7Xbev6bx0V1OZKJunGKN69IAZsnoFoCzfBcOA28HJFIwGWoOu1635YO2LSTyB7Gvwb8HZ_zyM8FW6ZHnzETm9okw2mX-Njq2G4YkQ4nzQuYA8sTeYA");
                            newRequest.Content = new StringContent(content, Encoding.UTF8, "application/json");
                            //Read Server Response
                            HttpResponseMessage response = await newClient.SendAsync(newRequest);
                            log.LogInformation($" Function successfully inserterted: ", response);

                        }
                        catch (Exception ex)
                        {
                            log.LogInformation($" Exception occured in function executed at: {DateTime.Now} ", ex.Message);

                        }
                    }
                }
            }


            //------------------------
           
        }
        //log

    }
    

}
