{-# LANGUAGE OverloadedStrings, DataKinds #-}

import Database.Cassandra.CQL
import Control.Monad
import Control.Monad.CatchIO
import Control.Monad.Trans (liftIO)
import Data.Int
import Data.Text (Text)
import qualified Data.Text as T
import Data.Decimal
import Data.UUID
import System.Random

dropLists :: Query Schema () ()
dropLists = "drop table decimals"

createLists :: Query Schema () ()
createLists = "create table decimals (id uuid PRIMARY KEY, item decimal)"

insert :: Query Write (UUID, Decimal) ()
insert = "insert into decimals (id, item) values (?, ?)"

select :: Query Rows UUID Decimal
select = "select item from decimals where id=?"

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
        executeWrite QUORUM insert (u1, read "0")
        executeWrite QUORUM insert (u2, read "1.02")
        executeWrite QUORUM insert (u3, read "12345678901234567890.123456789")
        executeWrite QUORUM insert (u4, read "-12345678901234567890.123456789")
        executeWrite QUORUM insert (u5, read "3.141592654")
        executeWrite QUORUM insert (u6, read "-3.141592654")
        executeWrite QUORUM insert (u7, read "-0.000000001")
        executeWrite QUORUM insert (u8, read "118000")
 
        let us = [u1,u2,u3,u4,u5,u6,u7, u8]
        forM_ us $ \u ->
            liftIO . print =<< executeRow QUORUM select u

