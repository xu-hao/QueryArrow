{-# LANGUAGE GADTs, FlexibleInstances, RankNTypes, StandaloneDeriving #-}
module QueryArrow.FileSystem.Serialization where

-- import Data.Aeson
-- import Data.Text (pack, unpack)
import QueryArrow.FileSystem.LocalCommands
import QueryArrow.Serialization ()
import Data.MessagePack
import Data.Time

data LocalizedCommandWrapper m where
   LocalizedCommandWrapper :: forall m a. (Show a, MessagePack a, Show (m a)) => m a -> LocalizedCommandWrapper m
   Exit :: LocalizedCommandWrapper m

deriving instance Show (LocalizedCommandWrapper m)

instance MessagePack File
instance MessagePack Stats

instance MessagePack UTCTime where
  toObject (UTCTime (ModifiedJulianDay day) daytime) = ObjectArray [toObject day, toObject (diffTimeToPicoseconds daytime)]
  fromObject (ObjectArray [a, b]) = do
    day <- fromObject a
    daytime <- fromObject b
    return (UTCTime (ModifiedJulianDay day) (picosecondsToDiffTime daytime))

instance MessagePack (LocalizedCommandWrapper LocalizedFSCommand) where
  toObject (LocalizedCommandWrapper (LDirExists a)) = ObjectArray [ toObject "LDirExists", toObject a ]
  toObject (LocalizedCommandWrapper (LFileExists a)) = ObjectArray [ toObject "LFileExists", toObject a ]
  toObject (LocalizedCommandWrapper (LUnlinkFile a)) = ObjectArray [ toObject "LUnlinkFile", toObject a ]
  toObject (LocalizedCommandWrapper (LRemoveDir a)) = ObjectArray [ toObject "LRemoveDir", toObject a ]
  toObject (LocalizedCommandWrapper (LMakeFile a)) = ObjectArray [ toObject "LMakeFile", toObject a ]
  toObject (LocalizedCommandWrapper (LMakeDir a)) = ObjectArray [ toObject "LMakeDir", toObject a ]
  toObject (LocalizedCommandWrapper (LWrite a b c)) =  ObjectArray [ toObject "LWrite", toObject (a, b, c) ]
  toObject (LocalizedCommandWrapper (LRead a b c)) =  ObjectArray [ toObject "LRead", toObject (a, b, c) ]
  toObject (LocalizedCommandWrapper (LListDirDir a)) = ObjectArray [ toObject "LListDirDir", toObject a ]
  toObject (LocalizedCommandWrapper (LListDirFile a)) = ObjectArray [ toObject "LListDirFile", toObject a ]
  toObject (LocalizedCommandWrapper LAllFiles) = ObjectArray [ toObject "LAllFiles" ]
  toObject (LocalizedCommandWrapper LAllDirs) = ObjectArray [ toObject "LAllDirs" ]
  toObject (LocalizedCommandWrapper LAllNonRootFiles) = ObjectArray [ toObject "LAllNonRootFiles" ]
  toObject (LocalizedCommandWrapper LAllNonRootDirs) = ObjectArray [ toObject "LAllNonRootDirs" ]
  toObject (LocalizedCommandWrapper (LFindFilesByName a)) = ObjectArray [ toObject "LFindFilesByName", toObject a ]
  toObject (LocalizedCommandWrapper (LFindDirsByName a)) = ObjectArray [ toObject "LFindDirsByName", toObject a ]
  toObject (LocalizedCommandWrapper (LFindFilesByPath a)) = ObjectArray [ toObject "LFindFilesByPath", toObject a ]
  toObject (LocalizedCommandWrapper (LFindDirsByPath a)) = ObjectArray [ toObject "LFindDirsByPath", toObject a ]
  toObject (LocalizedCommandWrapper (LFindFilesBySize a)) = ObjectArray [ toObject "LFindFilesBySize", toObject a ]
  toObject (LocalizedCommandWrapper (LFindFilesByModificationTime a)) = ObjectArray [ toObject "LFindFilesByModificationTime", toObject a ]
  toObject (LocalizedCommandWrapper (LFindDirsByModficationTime a)) = ObjectArray [ toObject "LFindDirsByModficationTime", toObject a ]
  toObject (LocalizedCommandWrapper (LStat a)) = ObjectArray [ toObject "LStat", toObject a ]
  toObject (LocalizedCommandWrapper (LTruncate a b)) = ObjectArray  [ toObject "Lpack Truncate", toObject (a, b) ]
  toObject (LocalizedCommandWrapper (LSize a)) = ObjectArray [ toObject "LSize", toObject a ]
  toObject (LocalizedCommandWrapper (LModificationTime a)) = ObjectArray [ toObject "LModificationTime", toObject a ]
  toObject Exit = ObjectArray [ toObject "Exit" ]
  fromObject (ObjectArray (d : t)) = do
                cmd <- fromObject d
                case cmd of
                  "LDirExists" -> (LocalizedCommandWrapper . LDirExists ) <$>   fromObject (head t)
                  "LFileExists" -> (LocalizedCommandWrapper . LFileExists) <$>   fromObject (head t)
                  "LUnlinkFile" -> (LocalizedCommandWrapper . LUnlinkFile) <$>   fromObject (head t)
                  "LRemoveDir" -> (LocalizedCommandWrapper . LRemoveDir ) <$>   fromObject (head t)
                  "LMakeFile" -> (LocalizedCommandWrapper .  LMakeFile) <$>fromObject (head t)
                  "LMakeDir" -> (LocalizedCommandWrapper . LMakeDir) <$>   fromObject (head t)
                  "LWrite" -> do
                    (a, b,  c) <- fromObject (head t)
                    return (LocalizedCommandWrapper (LWrite a b  c))
                  "LTruncate" -> do
                    (a,b) <- fromObject (head t)
                    return (LocalizedCommandWrapper (LTruncate  a b) )
                  "LRead" -> do
                    (a, b, c) <- fromObject (head t)
                    return (LocalizedCommandWrapper ( LRead a b c))
                  "LListDirDir" -> (LocalizedCommandWrapper . LListDirDir ) <$>    fromObject (head t)
                  "LListDirFile" ->  (LocalizedCommandWrapper . LListDirFile ) <$>fromObject (head t)
                  "LAllFiles" -> return  (LocalizedCommandWrapper LAllFiles )
                  "LAllDirs" -> return (LocalizedCommandWrapper LAllDirs)
                  "LAllNonRootFiles" -> return (LocalizedCommandWrapper LAllNonRootFiles)
                  "LAllNonRootDirs" -> return (LocalizedCommandWrapper LAllNonRootDirs)
                  "LFindFilesByName" -> (LocalizedCommandWrapper . LFindFilesByName) <$>    fromObject (head t)
                  "LFindDirsByName" -> (LocalizedCommandWrapper . LFindDirsByName ) <$>   fromObject (head t)
                  "LFindFilesByPath" -> (LocalizedCommandWrapper . LFindFilesByPath) <$>    fromObject (head t)
                  "LFindDirsByPath" -> (LocalizedCommandWrapper . LFindDirsByPath ) <$>    fromObject (head t)
                  "LFindFilesBySize" -> (LocalizedCommandWrapper . LFindFilesBySize ) <$>    fromObject (head t)
                  "LFindFilesByModificationTime" -> (LocalizedCommandWrapper . LFindFilesByModificationTime ) <$>   fromObject (head t)
                  "LFindDirsByModficationTime" -> (LocalizedCommandWrapper . LFindDirsByModficationTime ) <$>   fromObject (head t)
                  "LStat" -> (LocalizedCommandWrapper . LStat ) <$>   fromObject (head t)
                  "LSize" -> (LocalizedCommandWrapper . LSize ) <$>   fromObject (head t)
                  "LModificationTime" -> (LocalizedCommandWrapper . LModificationTime ) <$>   fromObject (head t)
                  "Exit" -> return Exit

{-
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
-}
