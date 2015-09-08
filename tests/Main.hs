{-# LANGUAGE OverloadedStrings, DataKinds #-}

import Control.Monad.CatchIO
import Control.Monad.Trans (liftIO)
import Data.Int
import qualified Data.List as L
import Data.Serialize hiding (Result)
import Data.Text (Text)
import Data.UUID
import Database.Cassandra.CQL
import System.Random
import Test.Tasty
import Test.Tasty.HUnit

tupleTests :: TestTree
tupleTests = testGroup "Tuple Tests" [testTupleInt2,testTuple3,testTuple4,testTuple5]

testTupleInt2 :: TestTree
testTupleInt2 = testCase "testTupleInt2" $ cassandraTest $ do
  ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM dropTupleI ()
  liftIO . print =<< executeSchema QUORUM createTupleI ()
  u1 <- liftIO randomIO
  u2 <- liftIO randomIO
  u3 <- liftIO randomIO
  executeWrite QUORUM insertI (u1, (10,11))
  executeWrite QUORUM insertI (u2, (2,4))
  executeWrite QUORUM insertI (u3, (900,1000))
  results <- executeRows QUORUM selectI ()
  liftIO $ do
    assertBool "Item Not Found" $ L.elem (u1, (10,11)) results
    assertBool "Item Not Found" $ L.elem (u2, (2,4)) results
    assertBool "Item Not Found" $ L.elem (u3, (900,1000)) results
  where
    dropTupleI :: Query Schema () ()
    dropTupleI = "drop table tuplei"

    createTupleI :: Query Schema () ()
    createTupleI = "create table tuplei (id uuid PRIMARY KEY, item tuple<int,int>)"

    insertI :: Query Write (UUID, (Int,Int)) ()
    insertI = "insert into tuplei (id, item) values (?, ?)"

    selectI :: Query Rows () (UUID,(Int,Int))
    selectI = "select id,item from tuplei"

testTuple3 :: TestTree
testTuple3 = testCase "testTuple3" $ cassandraTest $ do
  ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM dropTuple3 ()
  liftIO . print =<< executeSchema QUORUM createTuple3 ()
  u1 <- liftIO randomIO
  u2 <- liftIO randomIO
  u3 <- liftIO randomIO
  let tup1 = (u1, (10,11,"Text1"))
      tup2 = (u2, (2,4,"Text2"))
      tup3 = (u3, (900,1000,"Text3"))
  executeWrite QUORUM insert3 tup1
  executeWrite QUORUM insert3 tup2
  executeWrite QUORUM insert3 tup3
  results <- executeRows QUORUM select3 ()
  liftIO $ do
    assertBool "Item Not Found" $ L.elem tup1 results
    assertBool "Item Not Found" $ L.elem tup2 results
    assertBool "Item Not Found" $ L.elem tup3 results
  where
    dropTuple3 :: Query Schema () ()
    dropTuple3 = "drop table tuple3"

    createTuple3 :: Query Schema () ()
    createTuple3 = "create table tuple3 (id uuid PRIMARY KEY, item tuple<int,int,text>)"

    insert3 :: Query Write (UUID, (Int,Int, Text)) ()
    insert3 = "insert into tuple3 (id, item) values (?, ?)"

    select3 :: Query Rows () (UUID,(Int,Int,Text))
    select3 = "select id,item from tuple3"

testTuple4 :: TestTree
testTuple4 = testCase "testTuple4" $ cassandraTest $ do
  ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM dropTuple4 ()
  liftIO . print =<< executeSchema QUORUM createTuple4 ()
  u1 <- liftIO randomIO
  u2 <- liftIO randomIO
  u3 <- liftIO randomIO
  let tup1 = (u1, (10,11,"Text1","Text4"))
      tup2 = (u2, (2,4,"Text2","Text5"))
      tup3 = (u3, (900,1000,"Text3","Text6"))
  executeWrite QUORUM insert4 tup1
  executeWrite QUORUM insert4 tup2
  executeWrite QUORUM insert4 tup3
  results <- executeRows QUORUM select4 ()
  liftIO $ do
    assertBool "Item Not Found" $ L.elem tup1 results
    assertBool "Item Not Found" $ L.elem tup2 results
    assertBool "Item Not Found" $ L.elem tup3 results
  where
    dropTuple4 :: Query Schema () ()
    dropTuple4 = "drop table tuple4"

    createTuple4 :: Query Schema () ()
    createTuple4 = "create table tuple4 (id uuid PRIMARY KEY, item tuple<int,int,text,text>)"

    insert4 :: Query Write (UUID, (Int,Int, Text,Text)) ()
    insert4 = "insert into tuple4 (id, item) values (?, ?)"

    select4 :: Query Rows () (UUID,(Int,Int,Text,Text))
    select4 = "select id,item from tuple4"

testTuple5 :: TestTree
testTuple5 = testCase "testTuple5" $ cassandraTest $ do
  ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM dropTuple5 ()
  liftIO . print =<< executeSchema QUORUM createTuple5 ()
  u1 <- liftIO randomIO
  u2 <- liftIO randomIO
  u3 <- liftIO randomIO
  let tup1 = (u1, (10,11,"Text1","Text4",1))
      tup2 = (u2, (2,4,"Text2","Text5",2))
      tup3 = (u3, (900,1000,"Text3","Text6",3))
  executeWrite QUORUM insert5 tup1
  executeWrite QUORUM insert5 tup2
  executeWrite QUORUM insert5 tup3
  results <- executeRows QUORUM select5 ()
  liftIO $ do
    assertBool "Item Not Found" $ L.elem tup1 results
    assertBool "Item Not Found" $ L.elem tup2 results
    assertBool "Item Not Found" $ L.elem tup3 results
  where
    dropTuple5 :: Query Schema () ()
    dropTuple5 = "drop table tuple5"

    createTuple5 :: Query Schema () ()
    createTuple5 = "create table tuple5 (id uuid PRIMARY KEY, item tuple<int,int,text,text,int>)"

    insert5 :: Query Write (UUID, (Int,Int, Text,Text,Int)) ()
    insert5 = "insert into tuple5 (id, item) values (?, ?)"

    select5 :: Query Rows () (UUID,(Int,Int,Text,Text,Int))
    select5 = "select id,item from tuple5"

{- UDT Tests -}

data TestType = TestType {
  ttText :: Text,
  ttInt :: Int
} deriving (Eq)

instance CasType TestType where
  getCas = do
    x <- getOption
    y <- getOption
    return $ TestType x y
  putCas x = do
    putOption $ ttText x
    putOption $ ttInt x
  casType _ = CUDT [CText,CInt]

testUDT :: TestTree
testUDT = testCase "testUDT" $ cassandraTest $ do
  ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM dropUdt ()
  ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM dropTable ()
  ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM createUdt ()
  liftIO . print =<< executeSchema QUORUM createTable ()
  let x = TestType "test value" 54
  u1 <- liftIO randomIO
  executeWrite QUORUM insertUdt (u1,x)
  results <- executeRows QUORUM selectUdt ()
  liftIO $ assertBool "Item not found" $ L.elem (u1,x) results
  where
    dropTable :: Query Schema () ()
    dropTable = "drop table udt"

    dropUdt :: Query Schema () ()
    dropUdt = "drop type testtype"

    createUdt :: Query Schema () ()
    createUdt = "create type test.testtype(textField text, intField int)"

    createTable :: Query Schema () ()
    createTable = "create table udt(id uuid PRIMARY KEY, item frozen<testtype>)"

    insertUdt :: Query Write (UUID,TestType) ()
    insertUdt = "insert into udt (id,item) values (?,?)"

    selectUdt :: Query Rows () (UUID,TestType)
    selectUdt = "select id,item from udt"

cassandraTest :: Cas () -> IO ()
cassandraTest action = do
  pool <- newPool [("127.0.0.1", "9042")] "test" Nothing -- servers, keyspace, auth
  runCas pool action


ignoreDropFailure :: Cas () -> Cas ()
ignoreDropFailure code = code `catch` \exc -> case exc of
    ConfigError _ _ -> return ()  -- Ignore the error if the table doesn't exist
    Invalid _ _ -> return ()
    _               -> throw exc

allTests :: TestTree
allTests = testGroup "All Tests" [tupleTests,testUDT]

main :: IO ()
main = defaultMain allTests
