using System.Threading.Tasks;
using Newtonsoft.Json;

namespace System.Net.Http
{
    public static class HttpContentExtensions
    {
        public static async Task<T> ReadAsAsync<T>(this HttpContent content)
        {
            var type = typeof(T);
            string json = await content.ReadAsStringAsync();
            T value = (T)JsonConvert.DeserializeObject(json, type);
            return value;
        }

        public static async Task<object> ReadAsAsync(this HttpContent content, Type type)
        {
            string json = await content.ReadAsStringAsync();
            var value = JsonConvert.DeserializeObject(json, type);
            return value;
        }
    }
}