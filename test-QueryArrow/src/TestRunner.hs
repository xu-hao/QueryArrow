import System.Environment
import QueryArrow.Serialization
import System.Directory
import Control.Exception
import Network.Socket
import System.IO
import QueryArrow.RPC.Message
import Control.Monad


main :: IO ()
main = do
  [inp, udsaddr] <- getArgs
  files <- getDirectoryContents inp
  mapM_ (runTest udsaddr) files

runTest :: String -> String -> IO ()
runTest addr filepath =
  bracket
    (do
      sock <- socket AF_UNIX Stream defaultProtocol
      connect sock (SockAddrUnix addr)
      socketToHandle sock ReadWriteMode)
    hClose
    (\ h -> do
      cnt <- readFile filepath
      let testsuite = read cnt :: [(QuerySet, Maybe ResultSet)]
      mapM_ (\(qs, rs) -> do
        sendMsgPack h qs
        case rs of
          Nothing -> return ()
          Just _ -> do
            rs2 <- receiveMsgPack h :: IO (Maybe ResultSet)
            unless (show rs == show rs2) $ error ("expected " ++ show rs ++ " got " ++ show rs2)
        ) testsuite
      )
