{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleContexts, ExistentialQuantification, FlexibleInstances, OverloadedStrings #-}
module QueryArrow.Parser where

import QueryArrow.FO.Data
import QueryArrow.DB.DB

import Prelude hiding (lookup)
import Text.ParserCombinators.Parsec hiding (State)
import Control.Applicative ((<$>), (<*>), (<*), (*>))
import qualified Text.Parsec.Token as T
import qualified Data.Text as TE

lexer = T.makeTokenParser T.LanguageDef {
    T.commentStart = "/*",
    T.commentEnd = "*/",
    T.commentLine = "//",
    T.nestedComments = False,
    T.identStart = letter <|> char '_',
    T.identLetter = alphaNum <|> char '_',
    T.opStart = oneOf "=~|⊗⊕∧∨∀∃¬⟶𝟏𝟎⊤⊥",
    T.opLetter = oneOf "=~|⊗⊕∧∨∀∃¬⟶𝟏𝟎⊤⊥",
    T.reservedNames = ["commit", "insert", "return", "delete", "key", "object", "property", "input", "output", "rewrite", "predicate", "exists", "import", "export", "qualified", "all", "from", "except", "if", "then", "else", "one", "zero", "max", "min", "sum", "average", "count", "limit", "group", "order", "by", "asc", "desc", "let", "distinct", "integer", "text", "bytestring", "ref", "null"],
    T.reservedOpNames = ["=", "~", "|", "||", "⊗", "⊕", "‖", "∃", "¬", "⟶","𝟏","𝟎"],
    T.caseSensitive = True
}

identifier = T.identifier lexer
reserved = T.reserved lexer
reservedOp = T.reservedOp lexer

parens :: FOParser a -> FOParser a
parens = T.parens lexer

brackets :: FOParser a -> FOParser a
brackets = T.brackets lexer

braces :: FOParser a -> FOParser a
braces = T.braces lexer

integer = T.integer lexer
stringp = T.stringLiteral lexer
comma = T.comma lexer
semi = T.semi lexer
symbol = T.symbol lexer
whiteSpace = T.whiteSpace lexer
dot = T.dot lexer

-- parser
type FOParser = GenParser Char ()

casttypep :: FOParser CastType
casttypep = (reserved "text" >> return TextType) <|> (reserved "integer" >> return NumberType) <|> (reserved "bytestring" >> return ByteStringType) <|> (reserved "ref" >> RefType <$> identifier)

argp :: FOParser Expr
argp =
    (reserved "null" >> return NullExpr)
    <|> (CastExpr <$> casttypep <*> argp)
    <|> (VarExpr <$> Var <$> identifier)
    <|> (IntExpr . fromIntegral <$> integer)
    <|> (reserved "pattern" >> PatternExpr. TE.pack <$> stringp )
    <|> (StringExpr . TE.pack <$> stringp)

arglistp :: FOParser [Expr]
arglistp =
    parens (sepBy argp comma)

atomp :: FOParser Atom
atomp = do
    predname <- prednamep
    arglist <- arglistp
    -- let thepred = fromMaybe (error ("atomp: undefined predicate " ++ show predname ++ ", available " ++ show2 workspace)) (lookupObject predname workspace)
    return (Atom predname arglist)

negp :: FOParser ()
negp = reservedOp "~" <|> reservedOp "¬"

existsp :: FOParser ()
existsp = reserved "exists" <|> reservedOp "∃"

timesp :: FOParser ()
timesp = optional (reservedOp "⊗")

plusp :: FOParser ()
plusp = reservedOp "|" <|> reservedOp "⊕"

parp :: FOParser ()
parp = reservedOp "||" <|> reservedOp "‖"

onep :: FOParser ()
onep = reserved "one" <|> reservedOp "𝟏"

zerop :: FOParser ()
zerop  = reserved "zero" <|> reservedOp "𝟎"

rarrowp :: FOParser ()
rarrowp = optional (reservedOp "⟶")

litp :: FOParser Lit
litp = Lit <$> (negp *> return Neg <|> return Pos) <*> atomp

litsp :: FOParser [Lit]
litsp = many1 litp

neg :: Lit -> Lit
neg (Lit Pos a) = Lit Neg a
neg (Lit Neg a) = Lit Pos a

returnp :: FOParser [Var]
returnp = reserved "return" *> varsp

letp :: FOParser (Var, Summary)
letp = do
    v <- varp
    reservedOp "="
    s <- (reserved "max" >> Max <$> varp)
      <|> (reserved "min" >> Min <$> varp)
      <|> (reserved "sum" >> Max <$> varp)
      <|> (reserved "average" >> Min <$> varp)
      <|> (reserved "count" >> (try (reserved "distinct" >> CountDistinct <$> varp) <|> return Count))
    return (v,s)

formula1p :: FOParser Formula
formula1p = try (parens formulap)
       <|> (FAtomic <$> try atomp)
       <|> (Aggregate Not <$> (negp >> formula1p))
       <|> (Aggregate Exists <$> (existsp >> formula1p))
       <|> (Aggregate <$> (reserved "distinct" >> return Distinct) <*> formula1p)
       <|> (Aggregate <$> (Summarize <$> (reserved "let" >> sepBy1 letp comma) <*> ((reserved "group" >> reserved "by" >> many varp) <|> pure [])) <*> formula1p)
       <|> (Aggregate <$> (reserved "limit" >> Limit . fromIntegral <$> integer) <*> formula1p)
       <|> (Aggregate <$> (reserved "order" >> reserved "by" >> (try (OrderByAsc <$> varp <* reserved "asc") <|> (OrderByDesc <$> varp <* reserved "desc"))) <*> formula1p)
       <|> onep *> pure FOne
       <|> zerop *> pure FZero
       <|> (fsequencing . map FInsert) <$> (reserved "insert" >> litsp)
       <|> (fsequencing . map FInsert) <$> (reserved "delete" >> fmap (map neg) litsp)

formulap :: FOParser Formula
formulap = do
  formula1s <- sepBy1 formulaChoicep parp
  let formula1 = fpar formula1s
  (Aggregate . FReturn <$> returnp <*> pure formula1)
    <|> return formula1

formulaChoicep :: FOParser Formula
formulaChoicep = do
    formulaConjs <- sepBy1 formulaSequencingp plusp
    return (fchoice formulaConjs)


formulaSequencingp :: FOParser Formula
formulaSequencingp = do
  formulaConjs <- sepBy1 formula1p timesp
  return (fsequencing formulaConjs)

paramtypep :: FOParser ParamType
paramtypep = ParamType <$> ((reserved "key" >> return True) <|> return False) <*> ((reserved "input" >> return True) <|> return False) <*> ((reserved "output" >> return True) <|> return False) <*> casttypep

predtypep :: FOParser PredType
predtypep = PredType <$> (reserved "object" *> return ObjectPred <|> reserved "property" *> return PropertyPred) <*>
    parens (sepBy paramtypep comma)

prednamep :: FOParser PredName
prednamep = do
    name <- identifier
    (do
        _ <- dot
        name2 <- identifier
        return (QPredName name [] name2))
        <|> return (UQPredName name)

varp :: FOParser Var
varp = Var <$> identifier

varsp :: FOParser [Var]
varsp = many1 varp

progp :: FOParser [Command]
progp = do
    commands <- many (do
      whiteSpace
      (reserved "begin" *> return Begin)
          <|> (reserved "prepare" *> return Prepare)
          <|> (reserved "commit" *> return Commit)
          <|> (reserved "rollback" *> return Rollback)
          <|> (Execute <$> formulap))
    eof
    return commands
