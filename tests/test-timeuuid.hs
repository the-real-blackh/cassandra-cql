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
dropLists = "drop table timeuuids"

createLists :: Query Schema () ()
createLists = "create table timeuuids (id uuid PRIMARY KEY, item timeuuid)"

insertNow :: Query Write UUID ()
insertNow = "insert into timeuuids (id, item) values (?, now())"

insert :: Query Write (UUID, TimeUUID) ()
insert = "insert into timeuuids (id, item) values (?, ?)"

selectOne :: Query Rows UUID TimeUUID
selectOne = "select item from timeuuids where id=?"

select :: Query Rows () TimeUUID
select = "select item from timeuuids"

ignoreDropFailure :: Cas () -> Cas ()
ignoreDropFailure code = code `catch` \exc -> case exc of
    ConfigError _ -> return ()  -- Ignore the error if the table doesn't exist
    _             -> throw exc

main = do
    pool <- newPool [("localhost", "9042")] "test" -- servers, keyspace
    runCas pool $ do
        ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM dropLists ()
        liftIO . print =<< executeSchema QUORUM createLists ()

        u1 <- liftIO randomIO
        u2 <- liftIO randomIO
        executeWrite QUORUM insertNow u1
        Just t <- executeRow QUORUM selectOne u1
        executeWrite QUORUM insert (u2, t)

        liftIO . print =<< executeRows QUORUM select ()

