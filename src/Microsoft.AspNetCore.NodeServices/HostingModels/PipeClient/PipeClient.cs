using System;
using System.IO;
using System.IO.Pipes;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.AspNetCore.NodeServices.HostingModels.PipeClient {
    internal delegate void PipeClientReceivedLineHandler(string line);
    internal delegate void ServerDisconnectedHandler();
    internal delegate void PipeClientReceiveException(Exception ex);

    /**
     * A thread-safe network client that sends and receives lines of UTF-8 text.
     * On Windows, the transport is Named Pipes; on Linux/OSX it's Unix Domain Sockets.
     *
     * This is internal because the only intended use is for communicating with entrypoint-pipe.js
     * to support a fast .NET->Node RPC mechanism. 
     */
    internal class PipeClient : IDisposable {
        public bool IsServerDisconnected { get; private set; }
        public event ServerDisconnectedHandler ServerDisconnected;
        public event PipeClientReceivedLineHandler ReceivedLine;
        public event PipeClientReceiveException OnReceiveException;

        private bool useNamedPipes;
        private ConfiguredTaskAwaitable<bool> connected;
        private Socket unixSocket;
        private NamedPipeClientStream windowsNamedPipeClientStream;
        private CancellationTokenSource disposalCancellationTokenSource;
        private NetworkStream networkStream;
        private StreamWriter streamWriter;
        private StreamReader streamReader;
        private SemaphoreSlim streamWriterSemaphore = new SemaphoreSlim(1);
        private Exception isFaultedWithException;
        
        public PipeClient(string address) {
            this.useNamedPipes = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
            this.disposalCancellationTokenSource = new CancellationTokenSource();

            var connectionTcs = new TaskCompletionSource<bool>();
            this.connected = connectionTcs.Task.ConfigureAwait(false);
            this.ConnectAsync(address).ContinueWith(connectionTask => {
                if (connectionTask.IsFaulted) {
                    connectionTcs.SetException(connectionTask.Exception);
                } else {
                    this.streamReader = new StreamReader(connectionTask.Result);
                    this.streamWriter = new StreamWriter(connectionTask.Result);
                    this.streamWriter.AutoFlush = true;
                    this.BeginReceiveLoop();
                    connectionTcs.SetResult(true);
                }
            }, TaskContinuationOptions.RunContinuationsAsynchronously);
        }
        
        private Task<Stream> ConnectAsync(string address) {
            return this.useNamedPipes ? this.ConnectAsyncNamedPipe(address) : this.ConnectAsyncUnixDomainSocket(address);
        }
        
        private async Task<Stream> ConnectAsyncNamedPipe(string address) {
            this.windowsNamedPipeClientStream = new NamedPipeClientStream(".", address, PipeDirection.InOut);
            await this.windowsNamedPipeClientStream.ConnectAsync(this.disposalCancellationTokenSource.Token).ConfigureAwait(false);
            return this.windowsNamedPipeClientStream;
        }
        
        private async Task<Stream> ConnectAsyncUnixDomainSocket(string address) {
            var endPoint = new UnixDomainSocketEndPoint("/tmp/" + address);
            this.unixSocket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Unspecified);
            await this.unixSocket.ConnectAsync(endPoint).ConfigureAwait(false);
            this.networkStream = new NetworkStream(this.unixSocket);
            return this.networkStream;
        }
        
        public async Task SendAsync(string message) {
            if (this.isFaultedWithException != null) {
                throw new AggregateException("The PipeClient has already disconnected due to an exception in the receive loop", this.isFaultedWithException);
            }

            await this.connected;
            
            // TODO: Is it necessary to serialise WriteLineAsync calls like this? IIRC the Socket class at least allows
            // multiple queued writes, but then I also remember something about not having writes going through from
            // more than one thread at a time.
            await this.streamWriterSemaphore.WaitAsync(this.disposalCancellationTokenSource.Token).ConfigureAwait(false);
            try {
                await this.streamWriter.WriteLineAsync(message).ConfigureAwait(false);
            } finally {
                this.streamWriterSemaphore.Release();
            }
        }

        // This is 'async void' because it's purely an event loop. Nothing should listen for any result from any returned
        // task. Exceptions need to be reported via another channel, which in this case is the OnReceiveException event.
        // This doesn't include application-level exceptions (such as a PipeRpcClient invocation failing with a remote
        // exception) - those are handled as part of the RPC protocol. This only includes more severe system-level exceptions. 
        private async void BeginReceiveLoop() {
            try {
                while (true) {
                    string line;
                    try {
                        line = await this.streamReader.ReadLineAsync();    
                    } catch (ObjectDisposedException) {
                        // Client disconnected
                        return;
                    }
                    
                    if (line == null) {
                        // Server disconnected
                        this.IsServerDisconnected = true;
                        var serverDisconnectedEvt = this.ServerDisconnected;
                        if (serverDisconnectedEvt != null) {
                            serverDisconnectedEvt();
                        }
                        
                        return;
                    } else {
                        // Actually received a line
                        var evt = this.ReceivedLine;
                        if (evt != null) {
                            evt(line);
                        }
                    }
                }
            } catch (Exception ex) {
                // Since this is async void, for the important reason described above, we need to report exceptions
                // via another channel rather than a returned Task.
                var evt = this.OnReceiveException;
                if (evt != null) {
                    evt(ex);
                }
                this.isFaultedWithException = ex;
                this.Dispose(true);
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue) {
                if (disposing) {
                    this.disposalCancellationTokenSource.Cancel();
                    
                    this.ReceivedLine = null;
                    
                    if (this.streamWriter != null) {
                        this.streamWriter.Dispose();
                    }
                    
                    if (this.streamReader != null) {
                        this.streamReader.Dispose();
                    }
                    
                    if (this.networkStream != null) {
                        this.networkStream.Dispose();
                    }                

                    if (this.useNamedPipes) {
                        this.DisposeWindowsNamedPipeClientStream();
                    }
                    
                    if (this.unixSocket != null) {
                        this.unixSocket.Dispose();
                    }
                }

                disposedValue = true;
            }
        }
        
        private void DisposeWindowsNamedPipeClientStream() {
            if (this.windowsNamedPipeClientStream != null) {
                this.windowsNamedPipeClientStream.Dispose();
            }
        }

        public void Dispose() {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
        }
        #endregion
    }
}