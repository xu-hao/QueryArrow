module QueryArrow.DB.ElasticSearch.HTTP.ElasticSearchUtils where

-- http://swizec.com/blog/writing-a-rest-client-in-haskell/swizec/6152

import Network.HTTP.Conduit hiding (host, port)
import Control.Monad.IO.Class
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as B8
import Data.Aeson

get::(MonadIO m) => String -> m BL.ByteString
get url = simpleHttp url


-- http://stackoverflow.com/questions/33983629/basic-way-of-sending-http-post-in-haskell-using-http-conduit

buildPostRequest :: String -> RequestBody -> IO Request
buildPostRequest url body = do
  nakedRequest <- parseUrl url
  return (nakedRequest { method = B8.pack "POST", requestBody = body })

post :: String -> RequestBody -> IO BL.ByteString
post url s = do
  manager <- newManager tlsManagerSettings
  request <- buildPostRequest url s
  response <- httpLbs request manager
  return (responseBody response)

postJSON :: ToJSON a => String -> a -> IO BL.ByteString
postJSON url rec =
    post url (RequestBodyLBS (encode rec))

buildPutRequest :: String -> RequestBody -> IO Request
buildPutRequest url body = do
  nakedRequest <- parseUrl url
  return (nakedRequest { method = B8.pack "PUT", requestBody = body })

put :: String -> RequestBody -> IO BL.ByteString
put url s = do
  manager <- newManager tlsManagerSettings
  request <- buildPutRequest url s
  response <- httpLbs request manager
  return (responseBody response)

putJSON :: ToJSON a => String -> a -> IO BL.ByteString
putJSON url rec =
  put url (RequestBodyLBS (encode rec))


buildDeleteRequest :: String -> String -> IO Request
buildDeleteRequest url esid = do
  nakedRequest <- parseUrl (url ++ "/" ++ esid)
  return (nakedRequest { method = B8.pack "DELETE" })

delete :: String -> String -> IO BL.ByteString
delete url esid = do
  manager <- newManager tlsManagerSettings
  request <- buildDeleteRequest url esid
  response <- httpLbs request manager
  return (responseBody response)
