{-# LANGUAGE OverloadedStrings, DataKinds #-}

import Database.Cassandra.CQL
import Control.Applicative
import Control.Monad
import Control.Monad.CatchIO
import Control.Monad.Trans (liftIO)
import Data.Int
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock
import Data.UUID
import Network.Socket
import System.Random

dropLists :: Query Schema () ()
dropLists = "drop table inets"

createLists :: Query Schema () ()
createLists = "create table inets (id uuid PRIMARY KEY, item inet)"

insert :: Query Write (UUID, SockAddr) ()
insert = "insert into inets (id, item) values (?, ?)"

select :: Query Rows () SockAddr
select = "select item from inets"

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
        a1 <- liftIO $ addrAddress . head <$> getAddrInfo Nothing (Just "2406:e000:c182:1:949c:ae7d:64a1:1935") Nothing
        a2 <- liftIO $ addrAddress . head <$> getAddrInfo Nothing (Just "192.168.178.29") Nothing
        executeWrite QUORUM insert (u1, a1)
        executeWrite QUORUM insert (u2, a2)

        liftIO . print =<< executeRows QUORUM select ()

