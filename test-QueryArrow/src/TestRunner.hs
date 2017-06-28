import System.Environment
import QueryArrow.Serialization
import System.Directory
import Network.Socket
import System.IO
import QueryArrow.RPC.Message
import QueryArrow.Chopper.Data
import Test.Hspec
import Control.Monad
import System.Process
import Data.List
import Text.Parsec
import qualified Text.Parsec.Token as P
import Control.Monad.IO.Class
import qualified Data.Map as M
import Data.Time.Clock
import QueryArrow.FO.Data

normalizeResultSet :: ResultSet -> ResultSet
normalizeResultSet rset@(ResultSetError _) = rset
normalizeResultSet (ResultSetNormal rl) = ResultSetNormal (map (M.map (\(AbstractResultValue rv) -> AbstractResultValue (toConcreteResultValue rv))) rl)

sortResultSet :: ResultSet -> ResultSet
sortResultSet rset@(ResultSetError _) = rset
sortResultSet (ResultSetNormal rl) = ResultSetNormal (sort rl)

lexer :: P.GenTokenParser String () IO
lexer = P.makeTokenParser P.LanguageDef {
           P.reservedNames = ["skip"],
           P.commentStart = "",
           P.commentEnd = "",
           P.commentLine = "",
           P.nestedComments = False,
           P.identStart = letter,
           P.identLetter = letter <|> digit <|> oneOf "._-",
           P.opStart = oneOf "",
           P.opLetter = oneOf "",
           P.reservedOpNames = [],
           P.caseSensitive = True
           }

identifier :: ParsecT String () IO String
identifier = P.identifier lexer

integer :: ParsecT String () IO Integer
integer = P.integer lexer


reserved :: String -> ParsecT String () IO ()
reserved = P.reserved lexer

testp :: Handle -> Bool -> String -> Maybe Handle -> ParsecT String () IO ()
testp log dryrun addr h0 = do
  try eof <|> try (do
      reserved "skip"
      ref <- identifier
      liftIO $ putStrLn ("skipped " ++ ref)
      ) <|> try (do
      replicateM_ 10 (char '=')
      n <- integer
      act0 <- replicateM (fromInteger n) anyChar
      _ <- newline
      let act = read act0
      h1 <- liftIO $ case act of
        SendAction qs ->
                if dryrun
                          then do
                            putStrLn ("send " ++ show qs)
                            return h0
                          else do
                            h <- getConnection addr h0
                            hPutStrLn log ("send " ++ show qs)
                            sendMsgPack h qs
                            case qs of
                              QuerySet _ Quit _ -> do
                                hClose h
                                return Nothing
                              _ ->
                                return (Just h)
        RecvAction rs ->
                if dryrun
                  then do
                    putStrLn ("recv " ++ show rs)
                    return h0
                  else do
                    h <- getConnection addr h0
                    mRs2 <- receiveMsgPack h :: IO (Maybe ResultSet)
                    case mRs2 of
                      Nothing -> expectationFailure "cannot parse message"
                      Just rs2 -> 
                          let rs2' = normalizeResultSet rs2 in
                              -- if rs2' /= rs
                                  -- then 
                                      sortResultSet rs2' `shouldBe` sortResultSet rs
                                  -- else return ()
                    return (Just h)
      testp log dryrun addr h1)

getConnection :: String -> Maybe Handle -> IO Handle
getConnection addr h0 = do
        case h0 of
           Nothing -> connect1 addr
           Just h -> return h

connect1 :: String -> IO Handle
connect1 addr = do
                    sock <- socket AF_UNIX Stream defaultProtocol
                    connect sock (SockAddrUnix addr)
                    socketToHandle sock ReadWriteMode

connect2 :: Bool -> String -> IO (Maybe Handle)
connect2 dryrun addr =
                      if dryrun
                         then return Nothing
                         else Just <$> connect1 addr


main :: IO ()
main = do
  dryrun <- read <$> getEnv "qat_dryrun"
  udsaddr <- getEnv "qat_udsaddr"
  inp <- getEnv "qat_inp"
  list <- getEnv "qat"
  setupscript <- getEnv "qat_setup"
  files <- lines <$> readFile list
  logfilehandle <- openFile "/tmp/qat" WriteMode
  ti <- getCurrentTime
  hspec $ do
    describe "setup" $ do
      it "setup" $ do
        setup <- readFile setupscript
        let commands = lines setup
        h <- connect1 udsaddr
        mapM_ (\command -> if command == "" || ("//" `isPrefixOf` command)
                               then return ()
                               else do
                                   sendMsgPack h (QuerySet mempty (Dynamic command) mempty)
                                   mrset <- receiveMsgPack h :: IO (Maybe ResultSet)
                                   case mrset of
                                       Just (ResultSetError err) -> error (show err)
                                       Just (ResultSetNormal _) -> return ()
                                       Nothing -> error "cannot parse response") commands
        sendMsgPack h Quit
        hClose h
    describe "all tests" $ do
      zipWithM_ (\filepath n -> it filepath $ do
            t0 <- getCurrentTime
            cnt <- readFile (inp ++ "/" ++ filepath)
            h <- connect2 dryrun udsaddr
            res <- runParserT (testp logfilehandle dryrun udsaddr h) () filepath cnt
            res `shouldBe` Right ()
            t1 <- liftIO $ getCurrentTime
            liftIO $ putStrLn (show n ++ "/" ++ show (length files) ++ " " ++ show (diffUTCTime t1 t0) ++ " avg: " ++ show (diffUTCTime t1 ti / fromIntegral n))) files [1..]
  hClose logfilehandle
