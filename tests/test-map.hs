{-# LANGUAGE OverloadedStrings, DataKinds #-}

import Database.Cassandra.CQL
import Control.Monad
import Control.Monad.CatchIO
import Control.Monad.Trans (liftIO)
import Data.Int
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as C
import Data.Map (Map)
import qualified Data.Map as M
import Data.Text (Text)
import qualified Data.Text as T
import Data.UUID
import System.Random

dropLists :: Query Schema () ()
dropLists = "drop table maps"

createLists :: Query Schema () ()
createLists = "create table maps (id uuid PRIMARY KEY, items map<int,text>)"

insert :: Query Write (UUID, Map Int Text) ()
insert = "insert into maps (id, items) values (?, ?)"

select :: Query Rows () (Map Int Text)
select = "select items from maps"

ignoreDropFailure :: Cas () -> Cas ()
ignoreDropFailure code = code `catch` \exc -> case exc of
    ConfigError _ _ -> return ()  -- Ignore the error if the table doesn't exist
    _               -> throw exc

main = do
    pool <- createCassandraPool [("localhost", "9042")] "test" -- servers, keyspace
    runCas pool $ do
        ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM dropLists ()
        liftIO . print =<< executeSchema QUORUM createLists ()

        u1 <- liftIO randomIO
        u2 <- liftIO randomIO
        u3 <- liftIO randomIO
        executeWrite QUORUM insert (u1, M.fromList [(1, "one"), (2, "two")])
        executeWrite QUORUM insert (u2, M.fromList [(100, "hundred"), (200, "two hundred")])
        executeWrite QUORUM insert (u3, M.fromList [(12, "dozen")])

        liftIO . print =<< executeRows QUORUM select ()

