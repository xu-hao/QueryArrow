{-# LANGUAGE PatternSynonyms #-}

module QueryArrow.Syntax.Utils where


import QueryArrow.Syntax.Term
import QueryArrow.Syntax.Type

pattern PTKeyIO a = ParamType True True True False a
pattern PTKeyI a = ParamType True True False False a
pattern PTKeyO a = ParamType True False True False a
pattern PTPropIO a = ParamType False True True False a
pattern PTPropI a = ParamType False True False False a
pattern PTPropO a = ParamType False False True False a

pattern PTKeyIORef a = ParamType True True True True a
pattern PTKeyIRef a = ParamType True True False True a
pattern PTKeyORef a = ParamType True False True True a
pattern PTPropIORef a = ParamType False True True True a
pattern PTPropIRef a = ParamType False True False True a
pattern PTPropORef a = ParamType False False True True a

stripAnnotations :: Formula2 b a -> Formula2 b ()
stripAnnotations = fmap (const ())

addAnnotations :: Monoid a => Formula2 b () -> Formula2 b a
addAnnotations = fmap (const mempty)
