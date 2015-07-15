{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, ExistentialQuantification, FlexibleInstances, OverloadedStrings #-}
module Parser where

import FO
import Data.Map.Strict (Map, (!), member, insert)
import Text.ParserCombinators.Parsec
import Control.Applicative ((<$>), (<*>))

    
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
argp = 
    (VarExpr <$> identifier)
    <|> (IntExpr . read <$> many1 digit)
    <|> (char '\"' >> (StringExpr <$> many (noneOf "\"")) >>= \expr -> char '\"' >> return expr)

arglistp :: FOParser [Expr]
arglistp =
    oper '(' >> (
        (oper ')' >> return [])
        <|> ((:) <$> argp <*> arglisttail)
    ) where
        arglisttail :: FOParser [Expr]
        arglisttail = (spaces >> oper ')' >> return [])
            <|> (oper ',' >> spaces >> ((:) <$> argp <*> arglisttail))

atomp :: FOParser Atom
atomp = (do
    oper '('
    _ <- string "exists"
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
disjp = spaces >> ((:) <$> litp <*> disjptail) where
    disjptail = (spaces >> char '|' >> disjp) <|> return []
        
paramtypep :: FOParser Type
paramtypep = identifier

predtypep :: FOParser [Type]
predtypep =
    oper '(' >>
    ((oper ')' >> return [])
    <|> (:) <$> paramtypep <*> predtypetail) where
        predtypetail = (oper ')' >> return [])
            <|> (oper ',' >> spaces >> (:) <$> paramtypep <*> predtypetail)
    
predp :: FOParser ()
predp = do
    _ <- try (string "predicate")
    spaces
    name <- identifier
    spaces
    t <- predtypep
    let thepred = Pred name t
    updateState (insert name thepred)
    
    
disjsp :: FOParser [Disjunction]
disjsp = spaces >> ((predp >> disjsp)
    <|> (:) <$> disjp <*> disjsp
    <|> return [])
    
progp :: FOParser ([Disjunction], Map String Pred)
progp = do
    disjs <- disjsp
    predmap <- getState
    return (disjs, predmap)

       
    