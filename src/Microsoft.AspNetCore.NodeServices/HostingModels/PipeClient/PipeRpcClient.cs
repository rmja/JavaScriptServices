using System;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Microsoft.AspNetCore.NodeServices.HostingModels.PipeClient {
    /**
     * A thread-safe network client that implements a simple Remote Procedure Call (RPC) protocol,
     * where invocations and results/errors are JSON strings.
     *
     * This is loosely similar to JSON-RPC, but not the same, because JSON-RPC requires embedding
     * request/response IDs into the JSON body, which means you have to parse the JSON before you
     * know what type it's meant to be, which in turn makes it difficult to use a strongly-typed
     * JSON parsing API like JsonConvert.DeserializeObject<T>. Instead, here the request/response
     * IDs are managed by a lower-level string-based request/response protocol (see PipeRequestClient).
     *
     * This is internal because the only intended use is for communicating with entrypoint-pipe.js
     * to support a fast .NET->Node RPC mechanism. 
     */
     internal class PipeRpcClient : IDisposable {
        private readonly static JsonSerializerSettings jsonSerializerSettings =  new JsonSerializerSettings {
            ContractResolver = new CamelCasePropertyNamesContractResolver()
        };

        public bool IsServerDisconnected {
            get { return this.pipeRequestClient.IsServerDisconnected; }
        }
        
        private PipeRequestClient pipeRequestClient;

        public PipeRpcClient(string address) {
            this.pipeRequestClient = new PipeRequestClient(address);
        }
        
        public async Task<TResult> Invoke<TResult>(string method, params object[] args) {
            var requestJson = JsonConvert.SerializeObject(new {
                Method = method,
                Args = args
            }, jsonSerializerSettings);
            var responseJson = await this.pipeRequestClient.Request(requestJson).ConfigureAwait(false);
            var response = JsonConvert.DeserializeObject<PipeRpcResponse<TResult>>(responseJson, jsonSerializerSettings);
            
            if (response.ErrorMessage != null) {
                throw new PipeRpcClientException(response.ErrorMessage, response.ErrorDetails);
            } else {
                return response.Result;
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing) {
            if (!disposedValue) {
                if (disposing) {
                    this.pipeRequestClient.Dispose();
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
        }
        #endregion
        
        private class PipeRpcResponse<TResult> {
            public TResult Result;
            public string ErrorMessage;
            public string ErrorDetails;
        }
    }

    internal class PipeRpcClientException : Exception {
        public PipeRpcClientException(string message) : base(message) { }
        public PipeRpcClientException(string message, string details) : base(ConstructBaseMessage(message, details)) { }
        
        private static string ConstructBaseMessage(string message, string details) {
            if (string.IsNullOrEmpty(details)) {
                return "RPC call failed: " + message;
            } else {
                return "RPC call failed: " + message + Environment.NewLine + details;
            }
        }
    }
}