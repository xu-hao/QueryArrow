{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, FlexibleInstances, OverloadedStrings, GADTs, ExistentialQuantification, TemplateHaskell #-}

module QueryArrow.Data.Template where

import FO.Data
import DB.DB
import Translation
import Rewriting
import Config
import Utils
import Data.Namespace.Path
import SQL.ICAT

import Prelude hiding (lookup)
import Language.Haskell.TH hiding (Pred)
import Language.Haskell.TH.Syntax (VarBangType)
import Data.Char (toLower)
import Data.List (nub)

field :: String -> VarBangType
field x = (mkName x, Bang NoSourceUnpackedness NoSourceStrictness, ConT (mkName "Pred"))

struct :: [String] -> DecQ
struct xs = dataD (return []) (mkName "Predicates") [] Nothing [return (RecC (mkName "Predicates") (map field xs))] (return [])

pathE :: ObjectPath String -> ExpQ
pathE (ObjectPath (NamespacePath as) a) = do
    let l = listE (map stringE as)
    [|ObjectPath (NamespacePath $(l)) $(stringE a)|]

structval :: [(String,  String)] -> DecsQ
structval xs = do
      let pm = mkName "pm"
      let exprs = map (\(n, pn) -> do
          pval <- [|fromMaybe (error ("cannot find " ++ $(stringE n) ++ " from path " ++ $(stringE pn) ++ " available predicates " ++ show $(varE pm))) (lookup $(stringE pn) $(varE pm)) |]
          return (mkName n, pval)) xs
      let expr = recConE (mkName "Predicates") exprs
      [d| predicates pm = $(expr)|]

getRewritingRules :: String -> IO ([InsertRewritingRule], [InsertRewritingRule], [InsertRewritingRule])
getRewritingRules path = do
  transinfo <- getConfig path
  let ps = db_info (head (db_plugins transinfo))
  db <- makeICATSQLDBAdapter (db_namespace ps) (db_icat ps) (Just "nextid") ()
  let predmap0 = constructDBPredMap db
  putStrLn "predicates loaded "
  print predmap0
  -- trace ("preds:\n" ++ intercalate "\n" (map show (elems predmap0))) $ return ()
  (rewriting, _, _) <- getRewriting predmap0 transinfo
  return rewriting

getPredicates :: String -> IO [Pred]
getPredicates path = do
  transinfo <- getConfig path
  let ps = db_info (head (db_plugins transinfo))
  db <- makeICATSQLDBAdapter (db_namespace ps) (db_icat ps) (Just "nextid") ()
  return (getPreds db)

structs :: String -> DecsQ
structs path = do
    (qr, ir, dr) <- runIO (getRewritingRules path)
    preds <- runIO (getPredicates path)
    runIO $ mapM_ (\(Pred (ObjectPath _ n0) _) -> putStrLn ("generating struct field: _" ++ map toLower n0)) preds
    let n = (nub (map (\(InsertRewritingRule (Atom (Pred (ObjectPath _ n0) _) _) _) -> map toLower (drop 2 n0)) (qr ++ ir ++ dr)) ++ map (\(Pred (ObjectPath _ n0) _) -> "_" ++ map toLower n0) preds)
    let pn = (nub (map (\(InsertRewritingRule (Atom (Pred (ObjectPath _ n0) _) _) _) -> n0) (qr ++ ir ++ dr)) ++ map (\(Pred (ObjectPath _ n0) _) -> n0) preds)
    st <- struct n
    stv <- structval (zip n pn)
    return (st : stv)
