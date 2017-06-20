import System.Environment
import Text.Parsec
import qualified Text.Parsec.Token as P
import Text.Parsec.Language
import Control.Monad.IO.Class

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

type RoundTrip = (String, String)
type Test = (String, [RoundTrip])
type TestSuite = [Test]

testsuitep :: ParsecT String () IO TestSuite
testsuitep = do
  _ <- count 10 (char '*')
  reserved "ALL"
  reserved "TEST"
  reserved "BEGIN"
  r <- many testp
  _ <- count 10 (char '*')
  reserved "ALL"
  reserved "TEST"
  reserved "END"
  return r

testp :: ParsecT String () IO Test
testp = do
  _ <- count 10 (char '*')
  reserved "TEST"
  reserved "BEGIN"
  t <- identifier
  liftIO $ putStrLn ("parsing " ++ t ++ "...")
  _ <- count 10 (char '*')
  reserved "TEST"
  reserved "END"
  _ <- identifier
  r <- many roundtripp
  return (t, r)

roundtripp :: ParsecT String () IO RoundTrip
roundtripp = do
  _ <- brackets (many (noneOf "]"))
  _ <- many space
  _ <- count 10 (char '>')
  s <- many (noneOf "\n")
  _ <- newline
  _ <- brackets (many (noneOf "]"))
  _ <- many space
  _ <- count 10 (char '<')
  r <- many (noneOf "\n")
  _ <- newline
  return (s, r)



main :: IO ()
main = do
  [inp, out] <- getArgs
  putStrLn "reading file..."
  cnt <- readFile inp
  putStrLn "parsing file..."
  res <- runParserT testsuitep () inp cnt
  case res of
    Left err -> print err
    Right ts -> mapM_ (\(t, r) -> writeFile (out ++ "/" ++ t) (show r)) ts
  putStrLn "exiting..."
  
