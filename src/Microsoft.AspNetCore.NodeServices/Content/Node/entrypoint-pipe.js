// Limit dependencies to core Node modules. This means the code in this file has to be very low-level and unattractive,
// but simplifies things for the consumer of this module.
var fs = require('fs');
var net = require('net');
var path = require('path');
var readline = require('readline');
var useWindowsNamedPipes = /^win/.test(process.platform);

var parsedArgs = parseArgs(process.argv);
var listenAddress = (useWindowsNamedPipes ? '\\\\.\\pipe\\' : '/tmp/') + parsedArgs.pipename;

if (parsedArgs.watch) {
    autoQuitOnFileChange(process.cwd(), parsedArgs.watch.split(','));
}

var server = net.createServer()
    .on('listening', function() {
        // Signal to the NodeServices base class that we're ready to accept invocations
        console.log('[Microsoft.AspNetCore.NodeServices:Listening]');        
    })
    .on('connection', function(socket) {
        socket.on('error', function() {
            // Client disconnected ungracefully - nothing else to do.
            // This handler is needed to avoid the error being thrown as an exception.
        });
        
        readline.createInterface(socket, socket).on('line', function(line) {
            var req;
            try {
                req = parseRequest(line);

                var requestPayload = JSON.parse(req.data);
                switch (requestPayload.method) {
                    case 'invoke':
                        var invocation = requestPayload.args[0];
                        var invokedModule = require(path.resolve(process.cwd(), invocation.moduleName));
                        var func = invocation.exportedFunctionName ? invokedModule[invocation.exportedFunctionName] : invokedModule;
                        var invocationCallback = function(errorValue, successValue) {
                            sendResponse(socket, req.requestId, {
                                result: successValue,
                                errorMessage: errorValue && (errorValue.message || errorValue),
                                errorDetails: errorValue && (errorValue.stack || null)
                            })
                        };
                        func.apply(null, [invocationCallback].concat(invocation.args));
                        break;
                    default:
                        throw new Error('Unsupported method: ' + (requestPayload.method || '(none)'));
                }
            } catch (ex) {
                sendResponse(socket, req ? req.requestId : -1, {
                    errorMessage: ex.message,
                    errorDetails: ex.stack // TODO: Or is it stackTrace?
                });
            }
        });
    })
    .listen(listenAddress);

function sendResponse(client, requestId, responseObject) {
    client.write(requestId + ':' + JSON.stringify(responseObject) + '\n');
}

function parseRequest(line) {
    var colonIndex = line.indexOf(':');
    if (colonIndex < 0) {
        return { requestId: null, data: null };
    } else {
        return { requestId: line.substr(0, colonIndex), data: line.substr(colonIndex + 1) };
    }
}

function autoQuitOnFileChange(rootDir, extensions) {
    // Note: This will only work on Windows/OS X, because the 'recursive' option isn't supported on Linux.
    // Consider using a different watch mechanism (though ideally without forcing further NPM dependencies).
    var fs = require('fs');
    var path = require('path');
    fs.watch(rootDir, { persistent: false, recursive: true }, function(event, filename) {
        var ext = path.extname(filename);
        if (extensions.indexOf(ext) >= 0) {
            console.log('Restarting due to file change: ' + filename);
            process.exit(0);
        }
    });
}

function parseArgs(args) {
    // Very simplistic parsing which is sufficient for the cases needed. We don't want to bring in any external
    // dependencies (such as an args-parsing library) to this file.
    var result = {};
    var currentKey = null;
    args.forEach(function(arg) {
        if (arg.indexOf('--') === 0) {
            var argName = arg.substring(2);
            result[argName] = undefined;
            currentKey = argName;
        } else if (currentKey) {
            result[currentKey] = arg;
            currentKey = null;
        }
    });

    return result;
}