using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.AspNetCore.NodeServices.HostingModels.PipeClient {
    /**
     * A thread-safe network client that implements a simple request-response protocol in which
     * request and response payloads are arbitrary single-line UTF-8 strings.
     *
     * This is internal because the only intended use is for communicating with entrypoint-pipe.js
     * to support a fast .NET->Node RPC mechanism. 
     */
    internal class PipeRequestClient : IDisposable {
        public bool IsServerDisconnected {
            get { return this.pipeClient.IsServerDisconnected; }
        }

        internal PipeClientReceiveException OnReceiveException;

        private PipeClient pipeClient;
        private Dictionary<string, TaskCompletionSource<string>> outstandingRequests
         = new Dictionary<string, TaskCompletionSource<string>>();
        
        private int nextRequestId;
        
        public PipeRequestClient(string address) {
            this.pipeClient = new PipeClient(address);
            this.pipeClient.ReceivedLine += this.PipeClientReceivedLine;
            this.pipeClient.ServerDisconnected += () => {
                this.AbortAllOutstandingRequests("The server has disconnected");
            };

            // Propagate exceptions from the underlying receive loop. Such exceptions refer to infrastructure-level failures,
            // not application RPC exceptions.
            this.pipeClient.OnReceiveException += (ex) => {
                var evt = this.OnReceiveException;
                if (evt != null) {
                    evt(ex);
                }
            };
        }

        public Task<string> Request(string requestData) {
            if (this.disposedValue) {
                throw new ObjectDisposedException(nameof (PipeRequestClient));
            }
            
            if (requestData.IndexOf('\n') >= 0) {
                throw new ArgumentException("The request string must not contain a newline character", nameof(requestData));
            }
            
            if (this.IsServerDisconnected) {
                throw new PipeRequestException("Can't send request because the server already disconnected");
            }
            
            var reqId = Interlocked.Increment(ref this.nextRequestId).ToString();
            var tcs = new TaskCompletionSource<string>();
            lock (this.outstandingRequests) {
                this.outstandingRequests.Add(reqId, tcs);
            }
            
            this.pipeClient.SendAsync($"{ reqId }:{ requestData }").ContinueWith((task) => {
                // If we failed to send the request, propagate the exception via the TaskCompletionSource
                var responseTcs = this.GetAndRemoveOutstandingRequest(reqId);
                if (responseTcs != null) {
                    responseTcs.SetException(task.Exception);
                }
            }, TaskContinuationOptions.OnlyOnFaulted);
            
            return tcs.Task;
        }

        private void PipeClientReceivedLine(string line)
        {
            var colonIndex = line.IndexOf(':');
            if (colonIndex < 0) {
                throw new PipeRequestException("Protocol error: Received line in unexpected format. No colon separator found.");
            }
            
            var messageId = line.Substring(0, colonIndex);
            var responseTcs = this.GetAndRemoveOutstandingRequest(messageId);
            if (responseTcs != null) {
                var responsePayload = line.Substring(colonIndex + 1);
                responseTcs.SetResult(responsePayload);
            }
        }
        
        private TaskCompletionSource<string> GetAndRemoveOutstandingRequest(string messageId) {
            TaskCompletionSource<string> responseTcs;
            lock (this.outstandingRequests) {
                if (this.outstandingRequests.TryGetValue(messageId, out responseTcs)) {
                    this.outstandingRequests.Remove(messageId);
                }
            }
            
            return responseTcs;
        }
        
        private void AbortAllOutstandingRequests(string message) {
            lock (this.outstandingRequests) {
                foreach (var responseTcs in this.outstandingRequests.Values) {
                    responseTcs.SetException(new PipeRequestException(message));
                }
            }            
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing) {
            if (!disposedValue) {
                if (disposing) {
                    this.AbortAllOutstandingRequests("The PipeRequestClient was disposed");
                    this.pipeClient.Dispose();
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
    }
    
    public class PipeRequestException : System.Exception {
        public PipeRequestException(string message) : base(message) {}
    }
}