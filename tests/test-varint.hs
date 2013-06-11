{-# LANGUAGE OverloadedStrings, DataKinds #-}

import Database.Cassandra.CQL
import Control.Monad
import Control.Monad.CatchIO
import Control.Monad.Trans (liftIO)
import Data.Int
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock
import Data.UUID
import System.Random

dropLists :: Query Schema () ()
dropLists = "drop table varints"

createLists :: Query Schema () ()
createLists = "create table varints (id uuid PRIMARY KEY, item varint)"

insert :: Query Write (UUID, Integer) ()
insert = "insert into varints (id, item) values (?, ?)"

select :: Query Rows UUID Integer
select = "select item from varints where id=?"

ignoreDropFailure :: Cas () -> Cas ()
ignoreDropFailure code = code `catch` \exc -> case exc of
    ConfigError _ -> return ()  -- Ignore the error if the table doesn't exist
    _             -> throw exc

main = do
    pool <- createCassandraPool [("localhost", "9042")] "test" -- servers, keyspace
    runCas pool $ do
        ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM dropLists ()
        liftIO . print =<< executeSchema QUORUM createLists ()

        u1 <- liftIO randomIO
        u2 <- liftIO randomIO
        u3 <- liftIO randomIO
        u4 <- liftIO randomIO
        u5 <- liftIO randomIO
        u6 <- liftIO randomIO
        u7 <- liftIO randomIO
        u8 <- liftIO randomIO
        u9 <- liftIO randomIO
        u10 <- liftIO randomIO
        executeWrite QUORUM insert (u1, 0)
        executeWrite QUORUM insert (u2, -1)
        executeWrite QUORUM insert (u3, 12345678901234567890123456789)
        executeWrite QUORUM insert (u4, -12345678901234567890123456789)
        executeWrite QUORUM insert (u5, -65537)
        executeWrite QUORUM insert (u6, -65536)
        executeWrite QUORUM insert (u7, -65535)
        executeWrite QUORUM insert (u8, -32769)
        executeWrite QUORUM insert (u9, -32768)
        executeWrite QUORUM insert (u10, -32767)
 
        let us = [u1,u2,u3,u4,u5,u6,u7,u8,u9,u10]
        forM_ us $ \u ->
            liftIO . print =<< executeRow QUORUM select u

