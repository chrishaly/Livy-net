using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Livy_net
{
    public enum SessionKind { spark, pyspark, pyspark3, sparkr };

    public class LivyClient : ILivyClient
    {
        private string livyUrl;
        private string user;
        private string password;
        private SessionKind kind;

        public LivyClient(string livyUrl, string user, string password, SessionKind kind = SessionKind.pyspark3)
        {
            this.livyUrl = livyUrl;
            this.user = user;
            this.password = password;
            this.kind = kind;
        }

        /// <summary>
        /// Creates a new interactive Scala, Python, or R shell in the cluster.
        /// </summary>
        /// <returns></returns>

        public async Task<Session> OpenSession()
        {
            var data = "{'kind': '" + kind.ToString() + "'}";
            const string RequestUri = "/livy/sessions";

            return await PostAsync<Session>(data, RequestUri);
        }

        public async Task<Batch> OpenBatch(string file, string className)
        {
            //var data = "{'file': '" + file + "' " +",'args':['2']}";
            var data = "{'file': '" + file + "','className': '" + className + "'}";
            const string RequestUri = "/livy/batches";

            return await PostAsync<Batch>(data, RequestUri);
        }

        /// <summary>
        /// Returns the state of session.
        /// </summary>
        /// <param name="sessionId"></param>
        /// <returns></returns>
        public async Task<Session> GetSessionState(string sessionId)
        {
            string requestUri = "/livy/sessions/" + sessionId;
            return await GetAsync<Session>(requestUri);
        }

        /// <summary>
        /// Returns the state of session.
        /// </summary>
        /// <param name="sessionId"></param>
        /// <returns></returns>
        public async Task<SessionsResponse> GetSessions()
        {
            string requestUri = "/livy/sessions";
            return await GetAsync<SessionsResponse>(requestUri);
        }

        /// <summary>
        /// Kills the Session job.
        /// </summary>
        /// <param name="sessionId"></param>
        /// <returns></returns>
        public Task CloseSession(string sessionId)
        {
            string requestUri = "/livy/sessions/" + sessionId;
            return DeleteAsync(requestUri);
        }

        /// <summary>
        /// Kills the Batch job.
        /// </summary>
        /// <param name="batchId"></param>
        /// <returns></returns>
        public Task CloseBatch(string batchId)
        {
            string requestUri = "/livy/batches/" + batchId;
            return DeleteAsync(requestUri);
        }

        /// <summary>
        /// Runs a statement in a specific session.Stament could be a Scala, Java or Phyton job
        /// </summary>
        /// <param name="sessionId"></param>
        /// <param name="statement"></param>
        /// <returns></returns>
        public Task<Statement> PostStatement(string sessionId, string statement)
        {
            var data = "{'code': '" + statement + "'}";
            var requestUri = "/livy/sessions/" + sessionId + "/statements";
            return PostAsync<Statement>(data, requestUri);
        }

        /// <summary>
        /// Returns all the statements in a session.
        /// </summary>
        /// <param name="sessionId"></param>
        /// <returns></returns>

        public Task<Statements> GetStatementsResult(string sessionId)
        {
            var requestUri = "/livy/sessions/" + sessionId + "/statements";
            return GetAsync<Statements>(requestUri);
        }

        /// <summary>
        /// Returns a specified statement in a session.
        /// </summary>
        /// <param name="sessionId"></param>
        /// <param name="stamentId"></param>
        /// <returns></returns>
        public Task<Statement> GetStatementResult(string sessionId, string statementId)
        {
            string requestUri = "/livy/sessions/" + sessionId + "/statements/" + statementId;
            return GetAsync<Statement>(requestUri);
        }

        public Task<Log> GetSessionLog(string sessionId)
        {
            string requestUri = "/livy/sessions/" + sessionId + "/logs";
            return GetAsync<Log>(requestUri);
        }

        /// <summary>
        /// Returns the batch session information.
        /// </summary>
        /// <param name="batchId"></param>
        /// <returns></returns>
        public Task<Batch> GetBatchState(string batchId)
        {
            string requestUri = "/livy/batches/" + batchId;
            return GetAsync<Batch>(requestUri);
        }

        /// <summary>
        /// Returns the state of session.
        /// </summary>
        /// <param name="sessionId"></param>
        /// <returns></returns>
        public async Task<BatchesResponse> GetBatches()
        {
            string requestUri = "/livy/batches";
            return await GetAsync<BatchesResponse>(requestUri);
        }

        private void ConfigureClient(HttpClient client)
        {
            client.BaseAddress = new Uri(livyUrl);

            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            var byteArray = Encoding.ASCII.GetBytes(user + ":" + password);
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));
            client.DefaultRequestHeaders.Add("X-Requested-By", user);
        }

        private async Task<object> SendAsync(Type resultType, string jsonData, string RequestUri, HttpMethod method)
        {
            object response = null;
            using (var client = new HttpClient())
            {
                ConfigureClient(client);

                var request = new HttpRequestMessage(method, RequestUri);
                if (jsonData != null)
                {
                    JToken jt = JToken.Parse(jsonData);
                    string formattedJson = jt.ToString();

                    var content = new StringContent(formattedJson, Encoding.UTF8, "application/json");
                    request.Content = content;
                }

                var result = await client.SendAsync(request);

                if (result.IsSuccessStatusCode)
                {
                    if (resultType == null)
                        return null;

                    response = await result.Content.ReadAsAsync(resultType).ConfigureAwait(false);
                }
                else
                {
                    var message = await result.Content.ReadAsStringAsync();
                    throw new Exception("Livy open session failed: code:" + result.StatusCode
                        + " reason:" + result.ReasonPhrase + " message: " + message);
                }
            }

            return response;
        }

        private Task<T> PostAsync<T>(string jsonData, string RequestUri)
        {
            return SendAsync<T>(jsonData, RequestUri, HttpMethod.Post);
        }

        private async Task<T> SendAsync<T>(string jsonData, string RequestUri, HttpMethod method)
        {
            return (T)await SendAsync(typeof(T), jsonData, RequestUri, method);
        }

        private Task<T> GetAsync<T>(string requestUri)
        {
            return SendAsync<T>(null, requestUri, HttpMethod.Get);
        }

        private Task DeleteAsync(string requestUri)
        {
            return SendAsync(null, null, requestUri, HttpMethod.Delete);
        }

    }
}
