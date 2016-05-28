-- https://gist.github.com/ijt/1052896
module Logging where

import System.IO (stderr, Handle)
import System.Log.Logger (rootLoggerName, setHandlers, updateGlobalLogger,
                          Priority(INFO), Priority(WARNING), infoM, debugM,
                          warningM, errorM, setLevel)
import System.Log.Handler.Simple (fileHandler, streamHandler, GenericHandler)
import System.Log.Handler (setFormatter)
import System.Log.Formatter

setup = do
    -- let logPath = "/tmp/foo.log"
    myStreamHandler <- streamHandler stderr INFO
    -- myFileHandler <- fileHandler logPath WARNING
    -- let myFileHandler' = withFormatter myFileHandler
    let myStreamHandler' = withFormatter myStreamHandler
    let log = rootLoggerName
    updateGlobalLogger log (setLevel INFO)
    updateGlobalLogger log (setHandlers [myStreamHandler'])

withFormatter :: GenericHandler Handle -> GenericHandler Handle
withFormatter handler = setFormatter handler formatter
    -- http://hackage.haskell.org/packages/archive/hslogger/1.1.4/doc/html/System-Log-Formatter.html
    where formatter = simpleLogFormatter "[$time $loggername $prio] $msg"
