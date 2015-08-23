{-# LANGUAGE OverloadedStrings, DataKinds #-}

import Control.Monad.CatchIO
import Control.Monad.Trans (liftIO)
import Data.Int
import qualified Data.List as L
import Data.UUID
import Database.Cassandra.CQL
import System.Random
import Test.Tasty
import Test.Tasty.HUnit

tupleTests :: TestTree
tupleTests = testGroup "Tuple Tests" [testTupleInt2]

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
  liftIO $ assertBool "Item Not Found" $ L.elem (u1, (10,11)) results
  where
    dropTupleI :: Query Schema () ()
    dropTupleI = "drop table tuplei"

    createTupleI :: Query Schema () ()
    createTupleI = "create table tuplei (id uuid PRIMARY KEY, item tuple<int,int>)"

    insertI :: Query Write (UUID, (Int,Int)) ()
    insertI = "insert into tuplei (id, item) values (?, ?)"

    selectI :: Query Rows () (UUID,(Int,Int))
    selectI = "select id,item from tuplei"


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
allTests = testGroup "All Tests" [tupleTests]

main :: IO ()
main = defaultMain allTests
