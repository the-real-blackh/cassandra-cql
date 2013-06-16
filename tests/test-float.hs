{-# LANGUAGE OverloadedStrings, DataKinds #-}

import Database.Cassandra.CQL
import Control.Monad
import Control.Monad.CatchIO
import Control.Monad.Trans (liftIO)
import Data.Int
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as C
import Data.Set (Set)
import qualified Data.Set as S
import Data.Text (Text)
import qualified Data.Text as T
import Data.UUID
import System.Random

dropLists :: Query Schema () ()
dropLists = "drop table floats"

createLists :: Query Schema () ()
createLists = "create table floats (id uuid PRIMARY KEY, item float)"

insert :: Query Write (UUID, Float) ()
insert = "insert into floats (id, item) values (?, ?)"

select :: Query Rows () Float
select = "select item from floats"

ignoreDropFailure :: Cas () -> Cas ()
ignoreDropFailure code = code `catch` \exc -> case exc of
    ConfigError _ _ -> return ()  -- Ignore the error if the table doesn't exist
    _               -> throw exc

main = do
    pool <- newPool [("localhost", "9042")] "test" -- servers, keyspace
    runCas pool $ do
        ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM dropLists ()
        liftIO . print =<< executeSchema QUORUM createLists ()

        u1 <- liftIO randomIO
        u2 <- liftIO randomIO
        u3 <- liftIO randomIO
        executeWrite QUORUM insert (u1, 100)
        executeWrite QUORUM insert (u2, 0.5)
        executeWrite QUORUM insert (u3, 3.141592654)

        liftIO . print =<< executeRows QUORUM select ()

