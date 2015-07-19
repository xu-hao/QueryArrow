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
        str = char '\"' >> (many (noneOf "\"")) <* char '\"'

arglistp :: FOParser [Expr]
arglistp =
    oper '(' >> sepBy argp (oper ',') <* oper ')'

atomp :: FOParser Atom
atomp = (do
    oper '('
    _ <- string "exists"
    space
    spaces
    var <- identifier
    oper '.'
    conj <- disjsp
    oper ')'
    return (Exists var conj)) <|> (do
    predmap <- getState
    predname <- identifier
    if not (predname `member` predmap) 
        then error ("undefined predicate " ++ predname) 
        else do
            let thepred = predmap ! predname
            arglist <- arglistp
            return (Atom thepred arglist))
    
litp :: FOParser Lit
litp = (oper '~' >> Lit <$> return Neg <*> atomp)
    <|> Lit <$> return Pos <*> atomp
    
disjp :: FOParser Disjunction
disjp = do
    l <- litp
    (do try (oper '|')
        ls <- sepBy litp (oper '|')
        return (l:ls))
        <|> return [l]
        
paramtypep :: FOParser Type
paramtypep = identifier

predtypep :: FOParser [Type]
predtypep =
    oper '(' >> sepBy paramtypep (oper ',') <* oper ')'
    
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
    
    
disjsp :: FOParser [Disjunction]
disjsp = many1 disjp
    
varsp :: FOParser [Var]
varsp = spaces >> (
            (:) <$> identifier <*> varsp
            <|> return [])

progp :: FOParser (Query, Map String Pred)
progp = do
    spaces
    q <- disjsp2
    spaces
    predmap <- getState
    return (uncurry Query q, predmap) where
        disjsp2 = (
            try (string "return" >> space >> spaces >> ((,) <$> varsp <*> return []))
            <|> (predp >> disjsp2)
            <|> second <$> ((:) <$> disjp) <*> disjsp2)
