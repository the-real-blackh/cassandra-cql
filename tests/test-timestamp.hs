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
dropLists = "drop table timestamps"

createLists :: Query Schema () ()
createLists = "create table timestamps (id uuid PRIMARY KEY, item timestamp)"

insert :: Query Write (UUID, UTCTime) ()
insert = "insert into timestamps (id, item) values (?, ?)"

select :: Query Rows () UTCTime
select = "select item from timestamps"

ignoreDropFailure :: Cas () -> Cas ()
ignoreDropFailure code = code `catch` \exc -> case exc of
    ConfigError _ _ -> return ()  -- Ignore the error if the table doesn't exist
    Invalid _ _ -> return ()
    _               -> throw exc

main = do
    --let auth = Just (PasswordAuthenticator "cassandra" "cassandra")
    let auth = Nothing
    pool <- newPool [("localhost", "9042")] "test" auth -- servers, keyspace, auth
    runCas pool $ do
        ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM dropLists ()
        liftIO . print =<< executeSchema QUORUM createLists ()

        u1 <- liftIO randomIO
        t <- liftIO getCurrentTime
        executeWrite QUORUM insert (u1, t)

        liftIO . print =<< executeRows QUORUM select ()
