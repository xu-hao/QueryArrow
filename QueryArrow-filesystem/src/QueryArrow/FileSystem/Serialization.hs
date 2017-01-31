{-# LANGUAGE GADTs, FlexibleInstances, RankNTypes, StandaloneDeriving #-}
module QueryArrow.FileSystem.Serialization where

import Data.Aeson
import Data.Text (pack, unpack)
import QueryArrow.FileSystem.LocalCommands
import QueryArrow.Serialization ()

data LocalizedCommandWrapper m where
   LocalizedCommandWrapper :: forall m a. (FromJSON a, ToJSON a, Show (m a)) => m a -> LocalizedCommandWrapper m
   Exit :: LocalizedCommandWrapper m

deriving instance Show (LocalizedCommandWrapper m)

instance ToJSON File
instance FromJSON File

instance ToJSON Stats
instance FromJSON Stats

instance ToJSON (LocalizedCommandWrapper LocalizedFSCommand) where
  toJSON (LocalizedCommandWrapper (LDirExists a)) = object [ pack "cmd" .= "LDirExists", pack "args" .= toJSON a ]
  toJSON (LocalizedCommandWrapper (LFileExists a)) = object [ pack "cmd" .= "LFileExists", pack "args" .= toJSON a ]
  toJSON (LocalizedCommandWrapper (LUnlinkFile a)) = object [ pack "cmd" .= "LUnlinkFile", pack "args" .= toJSON a ]
  toJSON (LocalizedCommandWrapper (LRemoveDir a)) = object [ pack "cmd" .= "LRemoveDir", pack "args" .= toJSON a ]
  toJSON (LocalizedCommandWrapper (LMakeFile a)) = object [ pack "cmd" .= "LMakeFile", pack "args" .= toJSON a ]
  toJSON (LocalizedCommandWrapper (LMakeDir a)) = object [ pack "cmd" .= "LMakeDir", pack "args" .= toJSON a ]
  toJSON (LocalizedCommandWrapper (LWrite a b c)) =  object [ pack "cmd"  .= "LWrite", pack "args" .= toJSON (a, b, c) ]
  toJSON (LocalizedCommandWrapper (LRead a b c)) =  object [ pack "cmd"  .= "LRead", pack "args" .= toJSON (a, b, c) ]
  toJSON (LocalizedCommandWrapper (LListDirDir a)) = object [ pack "cmd" .= "LListDirDir", pack "args" .= toJSON a ]
  toJSON (LocalizedCommandWrapper (LListDirFile a)) = object [ pack "cmd" .= "LListDirFile", pack "args" .= toJSON a ]
  toJSON (LocalizedCommandWrapper (LAllFiles)) = object [pack "cmd" .="LAllFiles" ]
  toJSON (LocalizedCommandWrapper (LAllDirs)) = object [ pack "cmd" .="LAllDirs" ]
  toJSON (LocalizedCommandWrapper (LAllNonRootFiles)) = object [pack "cmd" .="LAllNonRootFiles" ]
  toJSON (LocalizedCommandWrapper (LAllNonRootDirs)) = object [ pack "cmd" .="LAllNonRootDirs" ]
  toJSON (LocalizedCommandWrapper (LFindFilesByName a)) = object [ pack "cmd" .= "LFindFilesByName", pack "args" .= toJSON a ]
  toJSON (LocalizedCommandWrapper (LFindDirsByName a)) = object [ pack "cmd" .= "LFindDirsByName", pack "args" .= toJSON a ]
  toJSON (LocalizedCommandWrapper (LFindFilesByPath a)) = object [ pack "cmd" .= "LFindFilesByPath", pack "args" .= toJSON a ]
  toJSON (LocalizedCommandWrapper (LFindDirsByPath a)) = object [ pack "cmd" .= "LFindDirsByPath", pack "args" .= toJSON a ]
  toJSON (LocalizedCommandWrapper (LFindFilesBySize a)) = object [ pack "cmd" .= "LFindFilesBySize", pack "args" .= toJSON a ]
  toJSON (LocalizedCommandWrapper (LFindFilesByModificationTime a)) = object [ pack "cmd" .= "LFindFilesByModificationTime", pack "args" .= toJSON a ]
  toJSON (LocalizedCommandWrapper (LFindDirsByModficationTime a)) = object [ pack "cmd" .= "LFindDirsByModficationTime", pack "args" .= toJSON a ]
  toJSON (LocalizedCommandWrapper (LStat a)) = object [ pack "cmd" .= "LStat", pack "args" .= toJSON a ]
  toJSON (LocalizedCommandWrapper (LTruncate a b)) = object  [pack  "cmd" .= "Lpack Truncate", pack "args" .= toJSON (a, b) ]
  toJSON (LocalizedCommandWrapper (LSize a)) = object [ pack "cmd" .= "LSize", pack "args" .= toJSON a ]
  toJSON (LocalizedCommandWrapper (LModificationTime a)) = object [ pack "cmd" .= "LModificationTime", pack "args" .= toJSON a ]
  toJSON (Exit) = object [pack "cmd" .= "Exit"]

instance FromJSON (LocalizedCommandWrapper LocalizedFSCommand) where
  parseJSON = withObject "cmdobj" $ \obj -> do
                cmd <- unpack <$> (obj .: pack "cmd")
                case cmd of
                  "LDirExists" -> (LocalizedCommandWrapper . LDirExists ) <$>   (obj .: pack "args")
                  "LFileExists" -> (LocalizedCommandWrapper . LFileExists) <$>    (obj .: pack "args")
                  "LUnlinkFile" -> (LocalizedCommandWrapper . LUnlinkFile) <$>    (obj .: pack "args")
                  "LRemoveDir" -> (LocalizedCommandWrapper . LRemoveDir ) <$>   (obj .: pack "args")
                  "LMakeFile" -> (LocalizedCommandWrapper .  LMakeFile) <$>(   obj .: pack "args")
                  "LMakeDir" -> (LocalizedCommandWrapper . LMakeDir) <$>   (obj .: pack "args")
                  "LWrite" -> do
                    (a, b,  c) <- obj .: pack "args"
                    return (LocalizedCommandWrapper (LWrite a b  c))
                  "LTruncate" -> do
                    (a,b) <- obj .: pack "args"
                    return (LocalizedCommandWrapper (LTruncate  a b) )
                  "LRead" -> do
                    (a, b, c) <- obj .: pack "args"
                    return (LocalizedCommandWrapper ( LRead a b c))
                  "LListDirDir" -> (LocalizedCommandWrapper . LListDirDir ) <$>    (obj .: pack "args")
                  "LListDirFile" ->  (LocalizedCommandWrapper . LListDirFile ) <$>(  obj .: pack "args")
                  "LAllFiles" -> return  (LocalizedCommandWrapper LAllFiles )
                  "LAllDirs" -> return (LocalizedCommandWrapper LAllDirs)
                  "LAllNonRootFiles" -> return (LocalizedCommandWrapper LAllNonRootFiles)
                  "LAllNonRootDirs" -> return (LocalizedCommandWrapper LAllNonRootDirs)
                  "LFindFilesByName" -> (LocalizedCommandWrapper . LFindFilesByName) <$>    (obj .: pack "args")
                  "LFindDirsByName" -> (LocalizedCommandWrapper . LFindDirsByName ) <$>   (obj .: pack "args")
                  "LFindFilesByPath" -> (LocalizedCommandWrapper . LFindFilesByPath) <$>    (obj .: pack "args")
                  "LFindDirsByPath" -> (LocalizedCommandWrapper . LFindDirsByPath ) <$>    (obj .: pack "args")
                  "LFindFilesBySize" -> (LocalizedCommandWrapper . LFindFilesBySize ) <$>    (obj .: pack "args")
                  "LFindFilesByModificationTime" -> (LocalizedCommandWrapper . LFindFilesByModificationTime ) <$>   (obj .: pack "args")
                  "LFindDirsByModficationTime" -> (LocalizedCommandWrapper . LFindDirsByModficationTime ) <$>   (obj .: pack "args")
                  "LStat" -> (LocalizedCommandWrapper . LStat ) <$>   (obj .: pack "args")
                  "LSize" -> (LocalizedCommandWrapper . LSize ) <$>   (obj .: pack "args")
                  "LModificationTime" -> (LocalizedCommandWrapper . LModificationTime ) <$>   (obj .: pack "args")
                  "Exit" -> return Exit

instance ToJSON LocalizedFSCommand2
instance FromJSON LocalizedFSCommand2
