{-# LANGUAGE DeriveGeneric #-}

module QueryArrow.ElasticSearch.Query where

-- http://swizec.com/blog/writing-a-rest-client-in-haskell/swizec/6152

import Network.HTTP.Conduit hiding (host, port)
import Control.Monad.IO.Class
import qualified Data.ByteString.Lazy as BL
import Data.Aeson
import GHC.Generics
import QueryArrow.ElasticSearch.Record
import QueryArrow.ElasticSearch.QueryResult
import QueryArrow.HTTP.ElasticSearchUtils
import System.Log.Logger
import qualified Data.Text as Text
import Data.Text.Encoding

data ElasticSearchConnInfo = ElasticSearchConnInfo {
    host :: String,
    port :: Int,
    index :: String
}

esConnInfoToUrl :: ElasticSearchConnInfo -> Text.Text -> String
esConnInfoToUrl esci type0 = "http://" ++ host esci ++ ":" ++ show (port esci) ++ "/" ++ index esci ++ "/" ++ Text.unpack type0


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

data ESTermQueryItem = ESIntTermQuery  Text.Text Integer | ESStrTermQuery  Text.Text Text.Text deriving (Show)

instance ToJSON ESQuery
instance ToJSON ESBoolQuery
instance ToJSON ESMustQuery
instance ToJSON ESTermQuery

instance ToJSON ESTermQueryItem where
    toJSON (ESIntTermQuery attr val) = object [
        attr .= val ]
    toJSON (ESStrTermQuery attr val) = object [
        attr .= val ]


getESRecord :: (MonadIO m) => ElasticSearchConnInfo -> Text.Text -> String -> m (Maybe ESRecord)
getESRecord esci type0 esid =
    get (esConnInfoToUrl esci type0 ++ "/" ++ esid) >>= return . decode

postESRecord :: ElasticSearchConnInfo -> Text.Text -> ESRecord -> IO BL.ByteString
postESRecord esci type0 rec = postJSON (esConnInfoToUrl esci type0) rec

updateESRecord :: ElasticSearchConnInfo -> Text.Text -> String -> ESRecord -> IO BL.ByteString
updateESRecord esci type0 esid rec = putJSON (esConnInfoToUrl esci type0 ++ "/" ++ esid) rec

queryBySearch :: ElasticSearchConnInfo -> Text.Text -> ESQuery -> IO (Either String ESQueryResult)
queryBySearch esci type0 qu = do
        infoM "ElasticSearch" ("queryBySearch: " ++ Text.unpack (decodeUtf8 (BL.toStrict (encode qu))) ++ show qu)
        resp <- post (esConnInfoToUrl esci type0 ++ "/" ++ "_search") (RequestBodyLBS (encode qu))
        infoM "ElasticSearch" ("resp: " ++ Text.unpack (decodeUtf8 (BL.toStrict resp)))
        return (eitherDecode resp)

deleteById :: ElasticSearchConnInfo -> Text.Text -> String -> IO BL.ByteString
deleteById esci type0 esid = do
    infoM "ElasticSearch" ("deleteById: " ++ esid)
    resp <- delete (esConnInfoToUrl esci type0) esid
    infoM "ElasticSearch" ("resp: " ++ Text.unpack (decodeUtf8 (BL.toStrict resp)))
    return resp
