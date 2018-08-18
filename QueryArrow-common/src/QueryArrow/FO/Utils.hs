{-# LANGUAGE PatternSynonyms #-}

module QueryArrow.FO.Utils where


import QueryArrow.FO.Data
import QueryArrow.Syntax.Type
import QueryArrow.FO.TypeChecker
import Control.Comonad.Cofree
import Data.Map.Strict (empty)

pattern PTKeyIO a = ParamType True True True a
pattern PTKeyI a = ParamType True True False  a
pattern PTKeyO a = ParamType True False True a
pattern PTPropIO a = ParamType False True True a
pattern PTPropI a = ParamType False True False a
pattern PTPropO a = ParamType False False True a

stripAnnotations :: FormulaT -> Formula
stripAnnotations (_ :< a) = () :< (fmap stripAnnotations a)

-- addAnnotations :: Formula -> FormulaT
-- addAnnotations (Tie a) = Annotated empty (fmap addAnnotations a)
