{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, ExistentialQuantification, FlexibleInstances, OverloadedStrings #-}
module Parser where

import FO
import Data.Map.Strict (Map, (!), member, insert)
import Text.ParserCombinators.Parsec
import Control.Applicative ((<$>), (<*>), (<*), (*>))
import Control.Arrow (first, second)

-- parser
type FOParser = GenParser Char (Map String Pred)

identifier :: FOParser String
identifier  = do
    c  <- letter
    cs <- many (alphaNum <|> char '_')
    return (c:cs)

oper :: Char -> FOParser ()
oper ch = do
    spaces
    _ <- char ch
    spaces
    return ()

argp :: FOParser Expr
argp = (
    (VarExpr <$> identifier)
    <|> (IntExpr . read <$> many1 digit)
    <|> try (string "pattern" >> space >> spaces >> PatternExpr <$> str )
    <|> StringExpr <$> str) where
        str = char '\"' >> many (noneOf "\"") <* char '\"'

arglistp :: FOParser [Expr]
arglistp =
    oper '(' >> sepBy argp (oper ',') <* oper ')'

atomp :: FOParser Atom
atomp = do
    predname <- identifier
    arglist <- arglistp
    predmap <- getState
    if not (predname `member` predmap)
        then error ("undefined predicate " ++ predname)
        else do
            let thepred = predmap ! predname
            return (Atom thepred arglist)


litp :: FOParser Lit
litp = (oper '~' >> Lit <$> return Neg <*> atomp)
    <|> Lit <$> return Pos <*> atomp

litsp :: FOParser [Lit]
litsp = many1 litp

formula1p :: FOParser Formula
formula1p = (try (oper '(') >> formulap <* oper ')')
       <|> (Not <$> (try (oper '~') >> formulap))
       <|> (Exists <$> (try (string "exists" >> space) >> spaces >> identifier) <*> (oper '.' >> formulap))
       <|> (Atomic <$> try atomp)

formulaConjp :: FOParser Formula
formulaConjp = do
    formula1s <- many formula1p
    return (case formula1s of
        [a] -> a
        _ -> Conjunction formula1s)

formulap :: FOParser Formula
formulap = do
    formulaConjs <- sepBy formulaConjp (oper '|')
    return (case formulaConjs of
        [a] -> a
        _ -> Disjunction formulaConjs)

paramtypep :: FOParser ParamType
paramtypep = (try (string "key" >> space >> spaces >> return Key) <|> (string "property" >> space >> spaces >> return Property)) <*> identifier

predtypep :: FOParser PredType
predtypep = PredType <$> (try (string "object" >> space >> spaces >> return ObjectPred) <|> try (string "property" >> space >> spaces >> return PropertyPred)) <*>
    (oper '(' >> sepBy paramtypep (oper ',') <* oper ')')

predp :: FOParser ()
predp = do
    name <- try (do
        string "predicate"
        space
        spaces
        identifier)
    t <- predtypep
    let thepred = Pred name t
    updateState (insert name thepred)


varp :: FOParser Var
varp = spaces >> identifier <* spaces

varsp :: FOParser [Var]
varsp = many1 varp

data QueryVariant = Q Query | I Insert deriving Show

progp :: FOParser (QueryVariant, Map String Pred)
progp = do
    spaces
    q <- formulap
    spaces
    qv <- try (string "return" >> space >> spaces >> (Q <$> (Query <$> varsp <*> return q)))
      <|> (string "insert" >> space >> spaces >> (I <$> (Insert <$> litsp <*> return q)))
    predmap <- getState
    return (qv, predmap)
