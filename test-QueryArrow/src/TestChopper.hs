import System.Environment
import Text.Parsec
import qualified Text.Parsec.Token as P
import Control.Monad.IO.Class
import QueryArrow.Chopper.Data
import System.IO

lexer :: P.GenTokenParser String () IO
lexer = P.makeTokenParser P.LanguageDef {
           P.reservedNames = ["ALL", "TEST", "BEGIN", "END"],
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

brackets :: ParsecT String () IO a -> ParsecT String () IO a
brackets = P.brackets lexer

identifier :: ParsecT String () IO String
identifier = P.identifier lexer

reserved :: String -> ParsecT String () IO ()
reserved = P.reserved lexer

testsuitep :: String -> String -> String -> ParsecT String () IO ()
testsuitep pid outlist out = do
  _ <- count 10 (char '*')
  reserved "ALL"
  reserved "TEST"
  reserved "BEGIN"
  h <- liftIO $ openFile outlist WriteMode
  _ <- many (testp pid h out)
  liftIO $ hClose h
  _ <- count 10 (char '*')
  reserved "ALL"
  reserved "TEST"
  reserved "END"

testp :: String -> Handle -> String -> ParsecT String () IO ()
testp pid outlist out = do
  try (do
      _ <- count 10 (char '*')
      reserved "TEST"
      )
  reserved "BEGIN"
  t <- identifier
  liftIO $ putStrLn ("parsing " ++ t ++ "...")
  liftIO $ hPutStrLn outlist t
  _ <- count 10 (char '*')
  reserved "TEST"
  reserved "END"
  _ <- identifier
  h <- liftIO $ openFile (out ++ "/" ++ t) WriteMode
  _ <- many (actionp pid h)
  liftIO $ hClose h

actionp :: String -> Handle -> ParsecT String () IO ()
actionp pid h = do
  try (do
    a <- messagep '>' pid
    let str = show (SendAction (read a))
    liftIO $ do
      hPutStrLn h ("==========" ++ show (length str))
      hPutStrLn h str
    ) <|> (do
      b <- messagep '<' pid
      let str = show (RecvAction (read b))
      liftIO $ do
        hPutStrLn h ("==========" ++ show (length str))
        hPutStrLn h str
      )

type Message = String

messagep :: Char -> String -> ParsecT String () IO Message
messagep dir pid = do
  _ <- brackets (many (noneOf "]"))
  _ <- many space
  pid2 <- many digit
  _ <- many space
  if pid2 == pid
    then do
      _ <- count 10 (char dir)
      msg <- many (noneOf "\n")
      _ <- newline
      return msg
    else do
      _ <- many (noneOf "\n")
      _ <- newline
      messagep dir pid

main :: IO ()
main = do
  [inp, pid, outlist, out] <- getArgs
  putStrLn "reading file..."
  cnt <- readFile inp
  putStrLn "parsing file..."
  res <- runParserT (testsuitep pid outlist out) () inp cnt
  case res of
    Left err -> print err
    Right _ -> return ()
  putStrLn "exiting..."
