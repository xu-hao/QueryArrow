import System.Environment
import QueryArrow.Serialization
import System.Directory
import Network.Socket
import System.IO
import QueryArrow.RPC.Message
import QueryArrow.Chopper.Data
import Test.Hspec

main :: IO ()
main = do
  let dryrun = False
  let udsaddr = "/tmp/QueryArrow"
  let inp = "/tmp/qatests"
  files <- filter (\filepath -> filepath /= "." && filepath /= "..") <$> getDirectoryContents inp
  hspec $ do
    describe "all tests" $ do
      mapM_ (\filepath -> it filepath $ do
            cnt <- readFile (inp ++ "/" ++ filepath)
            let test = read cnt :: [Action]
            h <- connect2 dryrun udsaddr
            runTest dryrun udsaddr h test) files where
        connect2 dryrun addr =
          if dryrun
             then return Nothing
             else Just <$> connect1 addr
        connect1 addr = do
                 sock <- socket AF_UNIX Stream defaultProtocol
                 connect sock (SockAddrUnix addr)
                 socketToHandle sock ReadWriteMode
        runTest :: Bool -> String -> Maybe Handle -> [Action] -> IO ()
        runTest _ _ _ [] = return ()
        runTest dryrun addr h0 (act : rest) = do
          case act of
            SendAction qs -> do
                    h1 <- if dryrun
                              then do
                                putStrLn ("send " ++ show qs)
                                return h0
                              else do
                                h <- getConnection addr h0
                                sendMsgPack h qs
                                case qs of
                                  QuerySet _ Quit _ -> do
                                    hClose h
                                    return Nothing
                                  _ ->
                                    return h0
                    runTest dryrun addr h1 rest
            RecvAction rs -> do
                    if dryrun
                      then
                        putStrLn ("recv " ++ show rs)
                      else do
                        h <- getConnection addr h0
                        rs2 <- receiveMsgPack h :: IO (Maybe ResultSet)
                        show rs `shouldBe` show rs2
                    runTest dryrun addr h0 rest
        getConnection :: String -> Maybe Handle -> IO Handle
        getConnection addr h0 = do
                case h0 of
                   Nothing -> connect1 addr
                   Just h -> return h
