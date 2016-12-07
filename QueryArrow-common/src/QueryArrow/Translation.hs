{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, UndecidableInstances, FlexibleContexts #-}
module QueryArrow.Translation where

import QueryArrow.DB.DB
import QueryArrow.FO.Data
import QueryArrow.QueryPlan
import QueryArrow.Rewriting
import QueryArrow.Config
import QueryArrow.Parser
import QueryArrow.Utils
import QueryArrow.ListUtils

import Prelude  hiding (lookup)
import Data.ByteString.Lazy.UTF8 (toString)
import Data.Map.Strict (foldrWithKey)
import Control.Applicative ((<$>))
import Control.Monad.Except
import Control.Monad.Trans.Reader
import Control.Monad.Trans.Either
import qualified Data.ByteString.Lazy as B
import Text.ParserCombinators.Parsec hiding (State)
import Data.Namespace.Namespace
import Algebra.SemiBoundedLattice
import Algebra.Lattice
import Data.Set (toAscList, Set)
import Data.Monoid
import Language.Preprocessor.Cpphs (runCpphs, defaultCpphsOptions, CpphsOptions(..), defaultBoolOptions, BoolOptions(..))
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
    determinateVars (TransDB _ db _ _ )  = determinateVars db
    supported _ _ _ = True

instance (IDatabaseUniformDBFormula Formula db) => IDatabase1 (TransDB db) where
    type DBQueryType (TransDB db) = DBQueryType db
    translateQuery (TransDB _ db preds (qr, ir, dr) ) vars2 qu vars =
      let ptm = constructPredTypeMap preds in
          case runReaderT (checkQuery qu) ptm of
            Right () ->
                let qu' = rewriteQuery qr ir dr (Include vars2) qu vars in
                    case runReaderT (checkQuery qu') ptm of
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
    d1 <- runCpphs defaultCpphsOptions{includes = include_file_path ps, boolopts = defaultBoolOptions {locations = False}}  (rewriting_file_path ps) d0
    case runParser rulesp (predmap, mempty, mempty) "" d1 of
        Left err -> error (show err)
        Right ((qr, ir, dr), predmap, exports) -> do
            mapM_ print qr
            mapM_ print ir
            mapM_ print dr
            return ((qr, ir, dr), predmap, exports)
