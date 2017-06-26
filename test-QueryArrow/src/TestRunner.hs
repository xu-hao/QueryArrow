import System.Environment
import QueryArrow.Serialization
import System.Directory
import Network.Socket
import System.IO
import QueryArrow.RPC.Message
import QueryArrow.Chopper.Data
import Test.Hspec
import Control.Monad
import Data.Maybe

import Text.Parsec
import qualified Text.Parsec.Token as P
import Control.Monad.IO.Class

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

testp :: Bool -> String -> Maybe Handle -> ParsecT String () IO ()
testp dryrun addr h0 = do
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
      case act of
        SendAction qs -> do
                h1 <- liftIO $ if dryrun
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
                                return (Just h)
                testp dryrun addr h1
        RecvAction rs -> do
                h1 <- liftIO $ if dryrun
                  then do
                    putStrLn ("recv " ++ show rs)
                    return h0
                  else do
                    h <- getConnection addr h0
                    mRs2 <- receiveMsgPack h :: IO (Maybe ResultSet)
                    case mRs2 of
                      Nothing -> expectationFailure "cannot parse message"
                      Just rs2 -> show rs2 `shouldBe` show rs
                    return (Just h)
                testp dryrun addr h1)

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
  dryrun <- read <$> getEnv "dryrun"
  udsaddr <- getEnv "udsaddr"
  inp <- getEnv "inp"
  files0 <- getDirectoryContents inp
  files <- filterM (\filepath -> doesFileExist (inp ++ "/" ++ filepath)) files0
  hspec $ do
    describe "all tests" $ do
      mapM_ (\filepath -> it filepath $ do
            cnt <- readFile (inp ++ "/" ++ filepath)
            h <- connect2 dryrun udsaddr
            res <- runParserT (testp dryrun udsaddr h) () filepath cnt
            res `shouldBe` Right ()) files
