module QueryArrow.Chopper.Data where

import QueryArrow.Serialization

data Action = SendAction QuerySet | RecvAction ResultSet deriving (Show, Read)
type Test = (String, [Action])
type TestSuite = [Test]
