
using System.Collections.Generic;

namespace Livy_net
{

    /// <summary>
    /// A session represents an interactive shell
    /// </summary>
    public class SessionsResponse
    {
        public string from { get; set; }

        public string total { get; set; }

        public List<Session> sessions { get; set; }
    }
}