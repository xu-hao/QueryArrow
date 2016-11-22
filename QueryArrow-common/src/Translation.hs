{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, UndecidableInstances, FlexibleContexts #-}
module Translation where

import DB.DB
import FO.Data
import QueryPlan
import Rewriting
import Config
import Parser
import Utils
import ListUtils

import Prelude  hiding (lookup)
import Data.ByteString.Lazy.UTF8 (toString)
import Data.Map.Strict (foldrWithKey)
import Control.Applicative ((<$>))
import Control.Monad.Except
import qualified Data.ByteString.Lazy as B
import Text.ParserCombinators.Parsec hiding (State)
import Data.Namespace.Namespace
import Algebra.SemiBoundedLattice
import Algebra.Lattice
import Data.Set (toAscList, Set)
import Data.Monoid
-- exec query from dbname

-- getAllResults2 :: (MonadIO m, MonadBaseControl IO m, IResultRow row, Num (ElemType row), Ord (ElemType row)) => [AbstractDatabase row Formula] -> MSet Var -> Formula -> m [row]
-- getAllResults2 dbs rvars query = do
--     qp <- prepareQuery' dbs rvars query bottom
--     let (_, stream) = execQueryPlan ([], pure mempty) qp
--     getAllResultsInStream stream


-- queryPlan2 :: AbstractDBList row -> Set Var -> MSet Var -> Formula -> QueryPlan2
-- queryPlan2 dbs vars vars2  formula =
--     let qp = formulaToQueryPlan dbs formula
--         qp1 = simplifyQueryPlan qp
--         qp2 = calculateVars vars vars2 qp1 in
--         optimizeQueryPlan dbs qp2

-- rewriteQuery' :: (MonadIO m) => [InsertRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> MSet Var -> Formula -> Set Var -> m Formula
-- rewriteQuery' qr ir dr rvars qu0 vars = do
--     liftIO $ infoM "QA" ("original query: " ++ show qu0)
--     let qu = rewriteQuery  qr ir dr rvars qu0 vars
--     liftIO $ infoM "QA" ("rewritten query: " ++ show qu)
--     return qu
--

defaultRewritingLimit :: Int
defaultRewritingLimit = 100

type RewritingRuleSets = ([InsertRewritingRule],  [InsertRewritingRule], [InsertRewritingRule])

rewriteQuery :: [InsertRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> MSet Var -> Formula -> Set Var -> Formula
rewriteQuery  qr ir dr vars form ext = runNew (do
    registerVars (toAscList ((case vars of
        Include vars -> vars
        Exclude vars -> vars)  \/ freeVars form))
    rewrites defaultRewritingLimit ext   qr ir dr form)

data TransDB db = TransDB String db [Pred] RewritingRuleSets

instance (IDatabaseUniformDBFormula Formula db) => IDatabase0 (TransDB db) where
    type DBFormulaType (TransDB db) = Formula
    getName (TransDB name _ _ _ ) = name
    getPreds (TransDB _ _ predmap _ ) = predmap
    determinateVars (TransDB _ db _ _ ) vars  atom = determinateVars db vars atom
    supported _ _ _ = True

instance (IDatabaseUniformDBFormula Formula db) => IDatabase1 (TransDB db) where
    type DBQueryType (TransDB db) = DBQueryType db
    translateQuery (TransDB _ db _ (qr, ir, dr) ) vars2 qu vars =
        case runExcept (checkQuery qu) of
            Right () ->
                let qu' = rewriteQuery qr ir dr (Include vars2) qu vars in
                    case runExcept (checkQuery qu') of
                        Right () -> translateQuery db vars2 qu' vars
                        Left err -> error err
            Left err -> error err


instance (IDatabase db) => IDatabase2 (TransDB db) where
    newtype ConnectionType (TransDB db) = TransDBConnection (ConnectionType db)
    dbOpen (TransDB _ db  _ _ ) = TransDBConnection <$> dbOpen db

instance IDatabaseUniformDBFormula Formula db => IDatabase (TransDB db)

instance (IDatabase db) => IDBConnection0 (ConnectionType (TransDB db)) where
    dbClose (TransDBConnection db ) = dbClose db
    dbBegin (TransDBConnection db) = dbBegin db
    dbCommit (TransDBConnection db) = dbCommit db
    dbPrepare (TransDBConnection db) = dbPrepare db
    dbRollback (TransDBConnection db) = dbRollback db

instance (IDatabase db) => IDBConnection (ConnectionType (TransDB db)) where
    type QueryType (ConnectionType (TransDB db)) = QueryType (ConnectionType db)
    type StatementType (ConnectionType (TransDB db)) = StatementType (ConnectionType db)
    prepareQuery (TransDBConnection db) = prepareQuery db
    -- exec (TransDB _ dbs _ _  _ _ _) qp vars stream = snd (execQueryPlan dbs (vars, stream ) qp)

getRewriting :: PredMap -> TranslationInfo -> IO (RewritingRuleSets, PredMap, PredMap)
getRewriting predmap ps = do
    d0 <- toString <$> B.readFile (rewriting_file_path ps)
    case runParser rulesp (predmap, mempty, mempty) "" d0 of
        Left err -> error (show err)
        Right ((qr, ir, dr), predmap, exports) -> do
            mapM_ print qr
            mapM_ print ir
            mapM_ print dr
            return ((qr, ir, dr), predmap, exports)
