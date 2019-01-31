namespace Livy_net
{
    public class Batch
    {
        public int id { get; set; }

        public string state { get; set; }

        public string[] log { get; set; }

        public string appId { get; set; }
    }
}
