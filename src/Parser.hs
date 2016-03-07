{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleContexts, ExistentialQuantification, FlexibleInstances, OverloadedStrings #-}
module Parser (progp, QueryVariant(..), Completion, transformDeletion) where

import FO.Data
import FO
import QueryPlan

import Prelude hiding (lookup)
import Data.Map.Strict (Map, (!), member, insert, lookup, fromList)
import Text.ParserCombinators.Parsec hiding (State)
import Data.Maybe
import Control.Applicative ((<$>), (<*>), (<*), (*>))
import Data.List (union)
import qualified Text.Parsec.Token as T

lexer = T.makeTokenParser T.LanguageDef {
    T.commentStart = "/*",
    T.commentEnd = "*/",
    T.commentLine = "//",
    T.nestedComments = False,
    T.identStart = letter <|> char '_',
    T.identLetter = alphaNum <|> char '_',
    T.opStart = oneOf "~|",
    T.opLetter = oneOf "~|",
    T.reservedNames = ["insert", "return", "delete", "key", "object", "properyt", "predicate", "exists", "forall"],
    T.reservedOpNames = ["~", "|"],
    T.caseSensitive = True
}

identifier = T.identifier lexer
reserved = T.reserved lexer
reservedOp = T.reservedOp lexer

parens :: FOParser a -> FOParser a
parens = T.parens lexer

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
    <|> (reserved "pattern" >> PatternExpr <$> stringp )
    <|> (StringExpr <$> stringp)

arglistp :: FOParser [Expr]
arglistp =
    parens (sepBy argp comma)

atomp :: FOParser Atom
atomp = do
    predname <- identifier
    arglist <- arglistp
    predmap <- getState
    let thepred = fromMaybe (error ("undefined predicate " ++ predname)) (lookup predname predmap)
    return (Atom thepred arglist)


litp :: FOParser Lit
litp = Lit <$> (reservedOp "~" *> return Neg <|> return Pos) <*> atomp

litsp :: FOParser [Lit]
litsp = many1 litp

formula1p :: FOParser Formula
formula1p = try (parens formulap)
       <|> (Not <$> (reservedOp "~" >> formula1p))
       <|> (Exists <$> (reserved "exists" >> Var <$> identifier) <*> (dot >> formulap))
       <|> (Forall <$> (reserved "forall" >> Var <$> identifier) <*> (dot >> formulap))
       <|> (Atomic <$> try atomp)

formulaConjp :: FOParser Formula
formulaConjp = do
    formula1s <- many formula1p
    return (case formula1s of
        [a] -> a
        _ -> Conjunction formula1s)

formulap :: FOParser Formula
formulap = do
    formulaConjs <- sepBy formulaConjp (reservedOp "|")
    return (case formulaConjs of
        [a] -> a
        _ -> Disjunction formulaConjs)

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

data QueryVariant = Q Query | I Insert  | D [Atom] Formula deriving Show

progp :: FOParser (QueryVariant, Map String Pred)
progp = do
    whiteSpace
    q <- formulap
    qv <- reserved "return" *> (Q <$> (Query <$> varsp <*> return q))
      <|> reserved "insert" *> (I <$> (Insert <$> litsp <*> return q))
      <|> reserved "delete" *> (D <$> many atomp <*> return q)
    predmap <- getState
    return (qv, predmap)

type Completion = [(Atom, [Atom])]

completion :: Completion -> [Atom] -> NewEnv [Atom]
completion crs atoms =
    unions <$> mapM (completionAtom crs) atoms

completionAtom :: Completion -> Atom -> NewEnv [Atom]
completionAtom crs atom = completion crs =<< completionAtom1 crs atom

completionAtom1 :: Completion -> Atom -> NewEnv [Atom]
completionAtom1 crs atom = unions <$> mapM (completionAtom11 atom) crs

completionAtom11 :: Atom -> (Atom, [Atom]) -> NewEnv [Atom]
completionAtom11 atom (head, tails) = do
      let vars0 = freeVars head
      vars <- new vars0
      let s = fromList (zip vars0 vars)
      return (case unify atom head of
          Just s' ->
              subst s' (subst s tails)
          Nothing ->
              [])


samePredAndKey :: Atom -> Atom -> Bool
samePredAndKey (Atom p1@(Pred _ pt1) args1) (Atom p2@(Pred _ pt2) args2) | p1 == p2 =
    keyComponents pt1 args1 == keyComponents pt2 args2
samePredAndKey _ _ = False

unionPred :: [Atom] -> [Atom] -> [Atom]
unionPred as1 as2 =
    as1 ++ filter (not . samePredAndKeys as1) as2 where
        samePredAndKeys as1 atom = any (samePredAndKey atom) as1

transformDeletion :: Completion -> [Atom] -> Formula -> Insert
transformDeletion crs atoms q =
    let atoms' = atoms `union` (runNew (do
            registerVars (freeVars atoms `union` freeVars q)
            completion crs atoms)) in
        Insert (map (Lit Neg) atoms') (q & conj (map Atomic atoms))

transformAddition :: Completion -> [Atom] -> Formula -> Insert
transformAddition crs atoms q =
    let atoms' = atoms `unionPred` (runNew (do
            registerVars (freeVars atoms `union` freeVars q)
            completion crs atoms)) in
        Insert (map (Lit Pos) atoms') q
