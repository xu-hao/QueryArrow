{-# LANGUAGE PatternSynonyms #-}
module QueryArrow.FileSystem.Builtin where

import QueryArrow.FO.Data
import QueryArrow.FO.Utils

pattern FilePathPredName ns = QPredName ns [] "FILE_PATH"

pattern DirPathPredName ns = QPredName ns [] "DIR_PATH"

pattern FileNamePredName ns = QPredName ns [] "FILE_NAME"

pattern DirNamePredName ns = QPredName ns [] "DIR_NAME"

pattern FileSizePredName ns = QPredName ns [] "FILE_SIZE"

-- pattern FileCreateTimePredName ns = QPredName ns [] "FILE_CREATE_TIME"

-- pattern DirCreateTimePredName ns = QPredName ns [] "DIR_CREATE_TIME"

pattern FileModifyTimePredName ns = QPredName ns [] "FILE_MODIFY_TIME"

pattern DirModifyTimePredName ns = QPredName ns [] "DIR_MODIFY_TIME"

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

pattern FileSizePred ns = Pred (FileSizePredName ns) (PredType PropertyPred [PTKeyI (RefType "FileObject"), PTPropIO NumberType])

-- pattern FileCreateTimePred ns = Pred (FileCreateTimePredName ns) (PredType PropertyPred [PTKeyI (RefType "FileObject"), PTPropIO NumberType])

-- pattern DirCreateTimePred ns = Pred (DirCreateTimePredName ns) (PredType PropertyPred [PTKeyI (RefType "DirObject"), PTPropIO NumberType])

pattern FileModifyTimePred ns = Pred (FileModifyTimePredName ns) (PredType PropertyPred [PTKeyI (RefType "FileObject"), PTPropIO NumberType])

pattern DirModifyTimePred ns = Pred (DirModifyTimePredName ns) (PredType PropertyPred [PTKeyI (RefType "DirObject"), PTPropIO NumberType])

pattern FileObjectPred ns = Pred (FileObjectPredName ns) (PredType ObjectPred [PTKeyI (RefType "FileObject")])

pattern DirObjectPred ns = Pred (DirObjectPredName ns) (PredType ObjectPred [PTKeyI (RefType "DirObject")])

pattern NewFileObjectPred ns = Pred (NewFileObjectPredName ns) (PredType PropertyPred [PTKeyI TextType, PTPropO (RefType "FileObject")])

pattern NewDirObjectPred ns = Pred (NewDirObjectPredName ns) (PredType PropertyPred [PTKeyI TextType, PTPropO (RefType "DirObject")])

pattern FileContentPred ns = Pred (FileContentPredName ns) (PredType PropertyPred [PTKeyI (RefType "FileObject"), PTPropIO (RefType "FileContent")])

pattern DirContentPred ns = Pred (DirContentPredName ns) (PredType PropertyPred [PTKeyI (RefType "DirObject"), PTKeyIO (RefType "DirContent")])

pattern DirDirPred ns = Pred (DirDirPredName ns) (PredType PropertyPred [PTKeyIO (RefType "DirObject"), PTPropIO (RefType "DirObject")])

pattern FileDirPred ns = Pred (FileDirPredName ns) (PredType PropertyPred [PTPropIO (RefType "FileObject"), PTPropIO (RefType "DirObject")])

pattern FileContentRangePred ns = Pred (FileContentRangePredName ns) (PredType PropertyPred [PTKeyI (RefType "FileContent"), PTPropI NumberType, PTPropI NumberType, PTPropIO ByteStringType])
