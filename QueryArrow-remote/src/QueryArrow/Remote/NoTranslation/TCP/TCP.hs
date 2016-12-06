{-# LANGUAGE RankNTypes, ScopedTypeVariables, TypeApplications, TypeFamilies, DataKinds, GADTs, FlexibleContexts #-}
module QueryArrow.Remote.NoTranslation.TCP.TCP where

import QueryArrow.DB.DB
import QueryArrow.DB.NoTranslation
import QueryArrow.FO.Data
import QueryArrow.Config
import QueryArrow.Remote.NoTranslation.Client
import Network
import QueryArrow.Remote.Channel
import QueryArrow.Remote.NoTranslation.Serialization ()

getDB :: ICATDBConnInfo -> IO (AbstractDatabase MapResultRow Formula)
getDB ps = do
    h <- connectTo (db_host ps) (PortNumber (fromIntegral (db_port ps)))
    db <- getQueryArrowClient (HandleChannel h)
    return (AbstractDatabase (NoTranslationDatabase db))
