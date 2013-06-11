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

dropSongs :: Query Schema () ()
dropSongs = "drop table songs"

createSongs :: Query Schema () ()
createSongs = "create table songs (id uuid PRIMARY KEY, title ascii, artist text, femaleSinger boolean, timesPlayed int, comment text)"

insertSong :: Query Write (UUID, ByteString, Text, Bool, Int, Maybe Text) ()
insertSong = "insert into songs (id, title, artist, femaleSinger, timesPlayed, comment) values (?, ?, ?, ?, ?, ?)"

getSongs :: Query Rows () (UUID, ByteString, Text, Bool, Int, Maybe Text)
getSongs = "select id, title, artist, femaleSinger, timesPlayed, comment from songs"

getOneSong :: Query Rows UUID (Text, Int)
getOneSong = "select artist, timesPlayed from songs where id=?"

ignoreDropFailure :: Cas () -> Cas ()
ignoreDropFailure code = code `catch` \exc -> case exc of
    ConfigError _ -> return ()  -- Ignore the error if the table doesn't exist
    _             -> throw exc

main = do
    {-
    Assuming a 'test' keyspace already exists. Here's some CQL to create it:
    CREATE KEYSPACE test WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };
    -}
    pool <- createCassandraPool [("localhost", "9042")] "test" -- servers, keyspace
    runCas pool $ do
        ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM dropSongs ()
        liftIO . print =<< executeSchema QUORUM createSongs ()

        u1 <- liftIO randomIO
        u2 <- liftIO randomIO
        u3 <- liftIO randomIO
        executeWrite QUORUM insertSong (u1, "La Grange", "ZZ Top", False, 2, Nothing)
        executeWrite QUORUM insertSong (u2, "Your Star", "Evanescence", True, 799, Nothing)
        executeWrite QUORUM insertSong (u3, "Angel of Death", "Slayer", False, 50, Just "Singer Tom Araya")

        songs <- executeRows QUORUM getSongs ()
        liftIO $ forM_ songs $ \(uuid, title, artist, female, played, mComment) -> do
            putStrLn ""
            putStrLn $ "id            : "++show uuid
            putStrLn $ "title         : "++C.unpack title
            putStrLn $ "artist        : "++T.unpack artist
            putStrLn $ "female singer : "++show female
            putStrLn $ "times played  : "++show played
            putStrLn $ "comment       : "++show mComment

        liftIO $ putStrLn ""
        --liftIO . print =<< executeRow QUORUM getOneSong u2
