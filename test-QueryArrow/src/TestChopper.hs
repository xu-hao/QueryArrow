import System.Environment
import Text.Parsec
import qualified Text.Parsec.Token as P
import Control.Monad.IO.Class
import QueryArrow.Serialization

lexer :: P.GenTokenParser String () IO
lexer = P.makeTokenParser P.LanguageDef {
           P.reservedNames = ["ALL", "TEST", "BEGIN", "END"],
           P.commentStart = "",
           P.commentEnd = "",
           P.commentLine = "",
           P.nestedComments = False,
           P.identStart = letter,
           P.identLetter = letter <|> digit <|> oneOf "._",
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

type RoundTrip = (QuerySet, Maybe ResultSet)
type Test = (String, [RoundTrip])
type TestSuite = [Test]

testsuitep :: String -> ParsecT String () IO TestSuite
testsuitep pid = do
  _ <- count 10 (char '*')
  reserved "ALL"
  reserved "TEST"
  reserved "BEGIN"
  r <- many (testp pid)
  _ <- count 10 (char '*')
  reserved "ALL"
  reserved "TEST"
  reserved "END"
  return r

testp :: String -> ParsecT String () IO Test
testp pid = do
  try (do
      _ <- count 10 (char '*')
      reserved "TEST"
      )
  reserved "BEGIN"
  t <- identifier
  liftIO $ putStrLn ("parsing " ++ t ++ "...")
  _ <- count 10 (char '*')
  reserved "TEST"
  reserved "END"
  _ <- identifier
  r <- many (roundtripp pid)
  return (t, r)

roundtripp :: String -> ParsecT String () IO RoundTrip
roundtripp pid = do
  a <- messagep '>' pid
  if a == "QuerySet {qsheaders = fromList [], qsquery = Quit, qsparams = fromList []}"
    then return (read a, Nothing)
    else do
      b <- messagep '<' pid
      return (read a, Just (read b))

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
  [inp, pid, out] <- getArgs
  putStrLn "reading file..."
  cnt <- readFile inp
  putStrLn "parsing file..."
  res <- runParserT (testsuitep pid) () inp cnt
  case res of
    Left err -> print err
    Right ts -> mapM_ (\(t, r) -> writeFile (out ++ "/" ++ t) (show r)) ts
  putStrLn "exiting..."
