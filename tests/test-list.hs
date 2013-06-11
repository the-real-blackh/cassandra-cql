{-# LANGUAGE OverloadedStrings, DataKinds #-}

import Database.Cassandra.CQL
import Control.Monad
import Control.Monad.CatchIO
import Control.Monad.Trans (liftIO)
import Data.Int
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as C
import Data.Text (Text)
import qualified Data.Text as T
import Data.UUID
import System.Random

dropListsI :: Query Schema () ()
dropListsI = "drop table listsi"

createListsI :: Query Schema () ()
createListsI = "create table listsi (id uuid PRIMARY KEY, items list<int>)"

insertI :: Query Write (UUID, [Int]) ()
insertI = "insert into listsi (id, items) values (?, ?)"

selectI :: Query Rows () [Int]
selectI = "select items from listsi"

dropListsT :: Query Schema () ()
dropListsT = "drop table listst"

createListsT :: Query Schema () ()
createListsT = "create table listst (id uuid PRIMARY KEY, items list<text>)"

insertT :: Query Write (UUID, [Text]) ()
insertT = "insert into listst (id, items) values (?, ?)"

selectT :: Query Rows () [Text]
selectT = "select items from listst"

ignoreDropFailure :: Cas () -> Cas ()
ignoreDropFailure code = code `catch` \exc -> case exc of
    ConfigError _ -> return ()  -- Ignore the error if the table doesn't exist
    _             -> throw exc

main = do
    pool <- createCassandraPool [("localhost", "9042")] "test" -- servers, keyspace
    runCas pool $ do
        do
            ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM dropListsI ()
            liftIO . print =<< executeSchema QUORUM createListsI ()

            u1 <- liftIO randomIO
            u2 <- liftIO randomIO
            u3 <- liftIO randomIO
            executeWrite QUORUM insertI (u1, [10,11,12])
            executeWrite QUORUM insertI (u2, [2,4,6,8])
            executeWrite QUORUM insertI (u3, [900,1000,1100])
    
            liftIO . print =<< executeRows QUORUM selectI ()

        do
            ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM dropListsT ()
            liftIO . print =<< executeSchema QUORUM createListsT ()
    
            u1 <- liftIO randomIO
            u2 <- liftIO randomIO
            u3 <- liftIO randomIO
            executeWrite QUORUM insertT (u1, ["dog","cat","rabbit"])
            executeWrite QUORUM insertT (u2, ["carrot","tomato"])
            executeWrite QUORUM insertT (u3, ["a","b","c"])
    
            liftIO . print =<< executeRows QUORUM selectT ()

