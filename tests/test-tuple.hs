{-# LANGUAGE OverloadedStrings, DataKinds #-}

import Control.Monad.CatchIO
import Control.Monad.Trans (liftIO)
import Data.Int
import Data.UUID
import Database.Cassandra.CQL
import System.Random

dropTupleI :: Query Schema () ()
dropTupleI = "drop table tuplei"

createTupleI :: Query Schema () ()
createTupleI = "create table tuplei (id uuid PRIMARY KEY, item tuple<int,int>)"

insertI :: Query Write (UUID, (Int,Int)) ()
insertI = "insert into tuplei (id, item) values (?, ?)"

selectI :: Query Rows () (UUID,(Int,Int))
selectI = "select id,item from tuplei"

ignoreDropFailure :: Cas () -> Cas ()
ignoreDropFailure code = code `catch` \exc -> case exc of
    ConfigError _ _ -> return ()  -- Ignore the error if the table doesn't exist
    Invalid _ _ -> return ()
    _               -> throw exc

main :: IO ()
main = do
    let auth = Nothing
    pool <- newPool [("127.0.0.1", "9042")] "test" auth -- servers, keyspace, auth
    runCas pool $ do
        ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM dropTupleI ()
        liftIO . print =<< executeSchema QUORUM createTupleI ()

        u1 <- liftIO randomIO
        u2 <- liftIO randomIO
        u3 <- liftIO randomIO
        executeWrite QUORUM insertI (u1, (10,11))
        executeWrite QUORUM insertI (u2, (2,4))
        executeWrite QUORUM insertI (u3, (900,1000))

        liftIO . print =<< executeRows QUORUM selectI ()

