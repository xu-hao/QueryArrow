{-# LANGUAGE DeriveGeneric #-}

module ElasticSearch.Query where

-- http://swizec.com/blog/writing-a-rest-client-in-haskell/swizec/6152

import Network.HTTP.Conduit hiding (host, port)
import Control.Monad.IO.Class
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy.Char8 as BL8
import Data.Aeson
import Control.Applicative
import Control.Monad.Trans
import GHC.Generics
import ElasticSearch.Record
import ElasticSearch.QueryResult
import HTTP.ElasticSearchUtils
import Debug.Trace
import qualified Data.Text as Text

data ElasticSearchConnInfo = ElasticSearchConnInfo {
    host :: String,
    port :: Int,
    index :: String,
    mapping :: String
}

esConnInfoToUrl :: ElasticSearchConnInfo -> String
esConnInfoToUrl esci = "http://" ++ host esci ++ ":" ++ show (port esci) ++ "/" ++ index esci ++ "/" ++ mapping esci


data ESQuery = ESQuery {
from :: Int,
size :: Int,
    query :: ESBoolQuery
} deriving (Show, Generic)
data ESBoolQuery = ESBoolQuery {
    bool :: ESMustQuery
} deriving (Show, Generic)
data ESMustQuery = ESMustQuery {
    must :: [ESTermQuery]
} deriving (Show, Generic)
data ESTermQuery = ESTermQuery {
    term :: ESTermQueryItem
} deriving (Show, Generic)

data ESTermQueryItem = ESIntTermQuery  String Int | ESStrTermQuery  String String deriving (Show)

instance ToJSON ESQuery
instance ToJSON ESBoolQuery
instance ToJSON ESMustQuery
instance ToJSON ESTermQuery

instance ToJSON ESTermQueryItem where
    toJSON (ESIntTermQuery attr val) = object [
        Text.pack attr .= val ]
    toJSON (ESStrTermQuery attr val) = object [
        Text.pack attr .= Text.pack val ]


getESRecord :: (MonadIO m) => ElasticSearchConnInfo -> String -> m (Maybe ESRecord)
getESRecord esci esid =
    get (esConnInfoToUrl esci ++ "/" ++ esid) >>= return . decode

postESRecord :: ElasticSearchConnInfo -> ESRecord -> IO BL.ByteString
postESRecord esci rec = postJSON (esConnInfoToUrl esci) rec

queryBySearch :: ElasticSearchConnInfo -> ESQuery -> IO (Either String ESQueryResult)
queryBySearch esci qu = do
        resp <- trace ("queryBySearch: " ++ BL8.unpack (encode qu) ++ show qu) $ post (esConnInfoToUrl esci ++ "/" ++ "_search") (RequestBodyLBS (encode qu))
        return (trace ("queryBySearch: resp " ++ BL8.unpack resp) $ eitherDecode resp)

deleteById :: ElasticSearchConnInfo -> String -> IO BL.ByteString
deleteById esci esid =
        delete (esConnInfoToUrl esci) esid
