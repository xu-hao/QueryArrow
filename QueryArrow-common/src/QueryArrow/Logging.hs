-- https://gist.github.com/ijt/1052896
module QueryArrow.Logging where

import System.IO (stderr, Handle)
import System.Log.Logger (rootLoggerName, setHandlers, updateGlobalLogger,
                          Priority(INFO), Priority(WARNING), infoM, debugM,
                          warningM, errorM, setLevel)
import System.Log.Handler.Simple (fileHandler, streamHandler, GenericHandler)
import System.Log.Handler (setFormatter)
import System.Log.Formatter

setup :: Priority -> IO ()
setup pri = do
    myStreamHandler <- streamHandler stderr pri
    let myStreamHandler' = withFormatter myStreamHandler
    let log = rootLoggerName
    updateGlobalLogger log (setLevel pri)
    updateGlobalLogger log (setHandlers [myStreamHandler'])
    let logPath = "/tmp/qatest.log"
    myFileHandler <- fileHandler logPath INFO
    let myFileHandler' = withFormatter myFileHandler
    updateGlobalLogger "TEST_LOG" $ (setLevel INFO . setHandlers [myFileHandler']) 

withFormatter :: GenericHandler Handle -> GenericHandler Handle
withFormatter handler = setFormatter handler formatter
    -- http://hackage.haskell.org/packages/archive/hslogger/1.1.4/doc/html/System-Log-Formatter.html
    where formatter = simpleLogFormatter "[$time $loggername $prio] $msg"
