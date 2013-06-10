{-# LANGUAGE OverloadedStrings #-}
import Database.Cassandra.CQL
import Control.Monad.Trans (liftIO)
import Data.Text (Text)

main = do
    pool <- createCassandraPool [("localhost", "9042")] "meta"
    runCas pool $ do
        q1 <- prepare "use meta"
        (liftIO . print) =<< execute q1 () QUORUM
        q2 <- prepare "drop table songs"
        (liftIO . print) =<< execute q2 () QUORUM
        q3 <- prepare "create table songs (title text PRIMARY KEY, artist text)"
        (liftIO . print) =<< execute q3 () QUORUM
        q4 <- prepare "insert into songs (title, artist) values (?, ?)"
        (liftIO . print) =<< execute q4 ("La Grange" :: Text, "ZZ Top" :: Text) QUORUM
        (liftIO . print) =<< execute q4 ("Your star" :: Text, "Evanescene" :: Text) QUORUM
        q6 <- prepare "select * from songs"
        (liftIO . print) =<< execute q6 () QUORUM

