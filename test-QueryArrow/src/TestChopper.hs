import System.IO
import System.Environment
import Text.Parsec
import Text.ParserCombinators.Parsec
import qualified Text.Parsec.Token as P
import Text.Parsec.Language

lexer = P.makeTokenParser emptyDef {
           reservedNames = ["ALL", "TEST", "BEGIN", "END"]
           }

brackets = P.brackets lexer
identifier = P.identifier lexer
reserved = P.identifier reserved

type RoundTrip = (String, String)
type Test = (String, [RoundTrip])
type TestSuite = [Test]

testsuitep :: Parsec () [TestSuite]
testsuitep = do
  _ <- count 10 (char '*')
  reserved "ALL"
  reserved "TEST"
  reserved "BEGIN"
  newline
  r <- many testp
  _ <- count 10 (char '*')
  reserved "ALL"
  reserved "TEST"
  reserved "END"
  newline
  return r

testp :: Parsec () [Test]
testp = do
  _ <- count 10 (char '*')
  reserved "TEST"
  reserved "BEGIN"
  t <- identifier
  newline
  _ <- count 10 (char '*')
  reserved "TEST"
  reserved "END"
  _ <- identifier
  newline
  r <- many roundtripp
  return (t, r)

roundtripp :: Parsec () [RoundTrip]
roundtripp = do
  _ <- brackets (noneOf "]")
  space
  _ <- count 10 (char '<')
  s <- many (noneOf "\n")
  newline
  _ <- brackets (noneOf "]")
  space
  _ <- count 10 (char '>')
  r <- many (noneOf "\n")
  newline
  return (s, r)



main :: IO ()
main = do
  [inp, out] <- getArgs
  case parse inp () of
    Left err -> print err
    Right ts -> mapM (\(t, r) -> writeFile ("out/"++ t) (show r)) ts
