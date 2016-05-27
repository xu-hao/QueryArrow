{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleContexts, ExistentialQuantification, FlexibleInstances, OverloadedStrings #-}
module Parser (progp, rulesp) where

import FO.Data
import QueryPlan
import Rewriting

import Prelude hiding (lookup)
import Data.Map.Strict (Map, (!), member, insert, lookup, fromList, keys)
import Text.ParserCombinators.Parsec hiding (State)
import Data.Maybe
import Control.Applicative ((<$>), (<*>), (<*), (*>))
import Data.List (union)
import qualified Text.Parsec.Token as T
import qualified Data.Text as TE

lexer = T.makeTokenParser T.LanguageDef {
    T.commentStart = "/*",
    T.commentEnd = "*/",
    T.commentLine = "//",
    T.nestedComments = False,
    T.identStart = letter <|> char '_',
    T.identLetter = alphaNum <|> char '_',
    T.opStart = oneOf "~|⊗⊕∧∨∀∃¬⟶𝟏𝟎⊤⊥",
    T.opLetter = oneOf "~|⊗⊕∧∨∀∃¬⟶𝟏𝟎⊤⊥",
    T.reservedNames = ["insert", "return", "delete", "key", "object", "property", "rewrite", "predicate", "exists", "forall", "if", "then", "else", "classical", "linear", "true", "false", "one", "zero"],
    T.reservedOpNames = ["~", "|", "⊗", "⊕", "∧", "∨", "∀", "∃", "¬", "⟶","𝟏","𝟎", "⊤", "⊥"],
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
type FOParser = GenParser Char (Map String Pred)

argp :: FOParser Expr
argp =
    (VarExpr <$> Var <$> identifier)
    <|> (IntExpr . fromIntegral <$> integer)
    <|> (reserved "pattern" >> PatternExpr. TE.pack <$> stringp )
    <|> (StringExpr . TE.pack <$> stringp)

arglistp :: FOParser [Expr]
arglistp =
    parens (sepBy argp comma)

atomp :: FOParser Atom
atomp = do
    predname <- identifier
    arglist <- arglistp
    predmap <- getState
    let thepred = fromMaybe (error ("atomp: undefined predicate " ++ predname ++ ", available " ++ show (keys predmap))) (lookup predname predmap)
    return (Atom thepred arglist)

negp :: FOParser ()
negp = reservedOp "~" <|> reservedOp "¬"

andp :: FOParser ()
andp = optional (reservedOp "∧")

orp :: FOParser ()
orp = reservedOp "|" <|> reservedOp "∨"

truep :: FOParser ()
truep = reserved "true" <|> reservedOp "⊤"

falsep :: FOParser ()
falsep = reserved "false" <|> reservedOp "⊥"

forallp :: FOParser ()
forallp = reserved "forall" <|> reservedOp "∀"

existsp :: FOParser ()
existsp = reserved "exists" <|> reservedOp "∃"

timesp :: FOParser ()
timesp = optional (reservedOp "⊗")

plusp :: FOParser ()
plusp = reservedOp "|" <|> reservedOp "⊕"

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

formula1p :: FOParser Formula
formula1p = try (parens formulap)
       <|> (FAtomic <$> try atomp)
       <|> (FTransaction <$> try (braces formulap))
       <|> (FClassical <$> try (brackets formulap'))
       <|> onep *> pure FOne
       <|> zerop *> pure FZero
       <|> (fsequencing . map FInsert) <$> (reserved "insert" >> litsp)
       <|> (fsequencing . map FInsert) <$> (reserved "delete" >> fmap (map neg) litsp)

formula1p' :: FOParser PureFormula
formula1p' = try (parens formulap')
      <|> (Not <$> (negp >> formula1p'))
      <|> (do
          existsp
          vars <- varsp
          _ <- dot
          form <- formulap'
          return (foldr Exists form vars))
      <|> (do
          forallp
          vars <- varsp
          _ <- dot
          form <- formulap'
          return (foldr Forall form vars))
      <|> (Atomic <$> try atomp)
      <|> truep *> pure CTrue
      <|> falsep *> pure CFalse

formulap :: FOParser Formula
formulap = do
  formula1s <- sepBy formulaSequencingp plusp
  return (fchoice formula1s)

formulaSequencingp :: FOParser Formula
formulaSequencingp = do
  formulaConjs <- sepBy formula1p timesp
  return (fsequencing formulaConjs)

formulaConjp :: FOParser PureFormula
formulaConjp = do
    formula1s <- sepBy formula1p' andp
    return (conj formula1s)

formulap' :: FOParser PureFormula
formulap' = do
    formulaConjs <- sepBy formulaConjp orp
    return (disj formulaConjs)


paramtypep :: FOParser ParamType
paramtypep = ((reserved "key" >> return Key) <|> (reserved "property" >> return Property)) <*> identifier

predtypep :: FOParser PredType
predtypep = PredType <$> (reserved "object" *> return ObjectPred <|> reserved "property" *> return PropertyPred) <*>
    parens (sepBy paramtypep comma)

predp :: FOParser ()
predp = do
    reserved "predicate"
    name <- identifier
    t <- predtypep
    let thepred = Pred name t
    updateState (insert name thepred)

varp :: FOParser Var
varp = Var <$> identifier

varsp :: FOParser [Var]
varsp = many1 varp

progp :: FOParser (Query, Map String Pred)
progp = do
    whiteSpace
    q <- formulap
    qv <- reserved "return" *> (Query <$> varsp <*> return q)
      <|> (Query [] <$> return q)
    eof
    predmap <- getState
    return (qv, predmap)


rulep :: FOParser ([QueryRewritingRule], [InsertRewritingRule], [InsertRewritingRule], [InsertRewritingRule])
rulep = (do
    whiteSpace
    _ <- many predp
    reserved "rewrite"
    reserved "insert" *> irulep
      <|> reserved "delete" *> drulep
      <|> reserved "classical" *> qrulep
      <|> qrule2p) where
          qrulep = do
              r <- QueryRewritingRule <$> atomp <* rarrowp <*> formulap'
              return ([r], [], [], [])
          qrule2p = do
              r <- InsertRewritingRule <$> atomp <* rarrowp  <*> formulap
              return ([], [r], [], [])
          irulep = do
              r <- InsertRewritingRule <$> atomp <* rarrowp  <*> formulap
              return ([], [], [r], [])
          drulep = do
              r <- InsertRewritingRule <$> atomp <* rarrowp  <*> formulap
              return ([], [], [], [r])

rulesp :: FOParser (([QueryRewritingRule], [InsertRewritingRule], [InsertRewritingRule], [InsertRewritingRule]), Map String Pred)
rulesp = do
    rules <- many rulep
    eof
    st <- getState
    return (mconcat rules, st)

samePredAndKey :: Atom -> Atom -> Bool
samePredAndKey (Atom p1 args1) (Atom p2 args2) | p1 == p2 =
    keyComponents p1 args1 == keyComponents p2 args2
samePredAndKey _ _ = False

unionPred :: [Atom] -> [Atom] -> [Atom]
unionPred as1 as2 =
    as1 ++ filter (not . samePredAndKeys as1) as2 where
        samePredAndKeys as1 atom = any (samePredAndKey atom) as1
