using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.NodeServices.HostingModels.PipeClient;

namespace Microsoft.AspNetCore.NodeServices {
    internal class PipeNodeInstance : OutOfProcessNodeInstance {
        private readonly string[] watchFileExtensions;
        private PipeRpcClient pipeRpcClient;
        private string pipeName;
        private object clientAccessLock = new object();
        
        public PipeNodeInstance(string projectPath, string[] watchFileExtensions = null)
            : base(EmbeddedResourceReader.Read(typeof(InputOutputStreamNodeInstance), "/Content/Node/entrypoint-pipe.js"), projectPath)
        {
            this.watchFileExtensions = watchFileExtensions;
		}
        
        public override async Task<T> Invoke<T>(NodeInvocationInfo invocationInfo)
        {
            await this.EnsureReady();
            
            Task<T> resultTask;
            lock (this.clientAccessLock) {
                if (this.pipeRpcClient == null) {
                    this.pipeRpcClient = new PipeRpcClient(this.pipeName);
                    this.pipeRpcClient.OnReceiveException += (ex) => {
                        // TODO: Also log the exception. Need to change the chain of calls up to this point to supply
                        // an ILogger or IServiceProvider etc.                        
                        this.ExitNodeProcess(); // We'll restart it next time there's a request to it
                    };
                }

                resultTask = this.pipeRpcClient.Invoke<T>("invoke", invocationInfo);
            }
            
            return await resultTask;
        }
        
        protected override void Dispose(bool disposing) {
            if (disposing) {
                this.EnsurePipeRpcClientDisposed();
            }

            base.Dispose(disposing);
        }
        
        protected override void OnBeforeLaunchProcess() {
            // Either we've never yet launched the Node process, or we did but the old one died.
            // Stop waiting for any outstanding requests and prepare to launch the new process.
            this.EnsurePipeRpcClientDisposed();
            this.pipeName = "pni-" + Guid.NewGuid().ToString("D"); // Arbitrary non-clashing string
            this.CommandLineArguments = MakeNewCommandLineOptions(this.pipeName, this.watchFileExtensions);
        }
        
        private static string MakeNewCommandLineOptions(string pipeName, string[] watchFileExtensions) {
            var result = "--pipename " + pipeName; 
            if (watchFileExtensions != null && watchFileExtensions.Length > 0) {
                result += " --watch " + string.Join(",", watchFileExtensions);
            }
            return result;
        }
        
        private void EnsurePipeRpcClientDisposed() {
            lock (this.clientAccessLock) {
                if (this.pipeRpcClient != null) {
                    this.pipeRpcClient.Dispose();
                    this.pipeRpcClient = null;
                }
            }
        }
    }
}