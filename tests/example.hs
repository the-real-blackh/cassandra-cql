{-# LANGUAGE OverloadedStrings #-}

import Database.Cassandra.CQL
import Control.Monad.CatchIO
import Control.Monad.Trans (liftIO)
import Data.Text (Text)

q1 :: Query ()
q1 = "use meta"

q2 :: Query ()
q2 = "drop table songs"

q3 :: Query ()
q3 = "create table songs (title text PRIMARY KEY, artist text)"

q4 :: Query (Text, Text)
q4 = "insert into songs (title, artist) values (?, ?)"

q5 :: Query ()
q5 = "select * from songs"

main = do
    pool <- createCassandraPool [("localhost", "9042")] "meta"
    runCas pool $ do
        (liftIO . print) =<< execute q1 () QUORUM
        do
            (liftIO . print) =<< execute q2 () QUORUM
          `catch` \exc -> case exc of
            ConfigError _ -> return ()  -- Ignore the error if the table doesn't exist
            _             -> liftIO $ throw exc
        (liftIO . print) =<< execute q3 () QUORUM
        (liftIO . print) =<< execute q4 ("La Grange", "ZZ Top") QUORUM
        (liftIO . print) =<< execute q4 ("Your star", "Evanescene") QUORUM
        (liftIO . print) =<< execute q4 ("Angel of Death", "Slayer") QUORUM
        (liftIO . print) =<< execute q5 () QUORUM
