{-# LANGUAGE PatternSynonyms #-}
module QueryArrow.FileSystem.Builtin where

import QueryArrow.FO.Data
import QueryArrow.FileSystem.Utils

pattern FilePathPredName ns = QPredName ns [] "FILE_PATH"

pattern DirPathPredName ns = QPredName ns [] "DIR_PATH"

pattern FileNamePredName ns = QPredName ns [] "FILE_NAME"

pattern DirNamePredName ns = QPredName ns [] "DIR_NAME"

pattern FileObjectPredName ns = QPredName ns [] "FILE_OBJ"

pattern DirObjectPredName ns = QPredName ns [] "DIR_OBJ"

pattern FileContentPredName ns = QPredName ns [] "FILE_CONTENT"

pattern DirContentPredName ns = QPredName ns [] "DIR_CONTENT"

pattern FileDirPredName ns = QPredName ns [] "FILE_DIR"

pattern DirDirPredName ns = QPredName ns [] "DIR_DIR"

pattern NewFileObjectPredName ns = QPredName ns [] "NEW_FILE_OBJ"

pattern NewDirObjectPredName ns = QPredName ns [] "NEW_DIR_OBJ"

pattern FileContentRangePredName ns = QPredName ns [] "FILE_CONTENT_RANGE"

pattern FilePathPred ns = Pred (FilePathPredName ns) (PredType PropertyPred [PTKeyO (RefType "FileObject"), PTPropI TextType])

pattern DirPathPred ns = Pred (DirPathPredName ns) (PredType PropertyPred [PTKeyO (RefType "DirObject"), PTPropI TextType])

pattern FileNamePred ns = Pred (FileNamePredName ns) (PredType PropertyPred [PTKeyI (RefType "FileObject"), PTPropIO TextType])

pattern DirNamePred ns = Pred (DirNamePredName ns) (PredType PropertyPred [PTKeyI (RefType "DirObject"), PTPropIO TextType])

pattern FileObjectPred ns = Pred (FileObjectPredName ns) (PredType ObjectPred [PTKeyI (RefType "FileObject")])

pattern DirObjectPred ns = Pred (DirObjectPredName ns) (PredType ObjectPred [PTKeyI (RefType "DirObject")])

pattern NewFileObjectPred ns = Pred (NewFileObjectPredName ns) (PredType PropertyPred [PTKeyI TextType, PTPropO (RefType "DirObject")])

pattern NewDirObjectPred ns = Pred (NewDirObjectPredName ns) (PredType PropertyPred [PTKeyI TextType, PTPropO (RefType "FileObject")])

pattern FileContentPred ns = Pred (FileContentPredName ns) (PredType PropertyPred [PTKeyI (RefType "FileObject"), PTPropIO (RefType "FileContnet")])

pattern DirContentPred ns = Pred (DirContentPredName ns) (PredType PropertyPred [PTKeyI (RefType "DirObject"), PTKeyIO (RefType "DirContent")])

pattern DirDirPred ns = Pred (DirDirPredName ns) (PredType PropertyPred [PTKeyIO (RefType "DirObject"), PTPropIO (RefType "DirObject")])

pattern FileDirPred ns = Pred (FileDirPredName ns) (PredType PropertyPred [PTPropIO (RefType "FileObject"), PTPropIO (RefType "DirObject")])

pattern FileContentRangePred ns = Pred (FileContentPredName ns) (PredType PropertyPred [PTKeyI (RefType "FileContent"), PTPropI NumberType, PTPropI NumberType, PTPropIO TextType])
