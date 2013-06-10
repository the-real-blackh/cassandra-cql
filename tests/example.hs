{-# LANGUAGE OverloadedStrings #-}

import Database.Cassandra.CQL
import Control.Monad.CatchIO
import Control.Monad.Trans (liftIO)
import Data.Int
import Data.Text (Text)

q2 :: Query Schema () ()
q2 = "drop table songs"

q3 :: Query Schema () ()
q3 = "create table songs (title text PRIMARY KEY, artist text)"

q4 :: Query Write (Text, Text) ()
q4 = "insert into songs (title, artist) values (?, ?)"

q5 :: Query Rows () (Text, Text)
q5 = "select * from songs"

main = do
    pool <- createCassandraPool [("localhost", "9042")] "meta"
    runCas pool $ do
        do
            (liftIO . print) =<< executeSchema q2 () QUORUM
          `catch` \exc -> case exc of
            ConfigError _ -> return ()  -- Ignore the error if the table doesn't exist
            _             -> liftIO $ throw exc
        (liftIO . print) =<< executeSchema q3 () QUORUM
        (liftIO . print) =<< executeWrite q4 ("La Grange", "ZZ Top") QUORUM
        (liftIO . print) =<< executeWrite q4 ("Your star", "Evanescence") QUORUM
        (liftIO . print) =<< executeWrite q4 ("Angel of Death", "Slayer") QUORUM
        (liftIO . print) =<< execute q5 () QUORUM
