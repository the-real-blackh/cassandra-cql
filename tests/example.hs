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
createSongs = "create table songs (id uuid PRIMARY KEY, title ascii, artist text, femaleSinger boolean, timesPlayed int)"

insertSong :: Query Write (UUID, ByteString, Text, Bool, Int) ()
insertSong = "insert into songs (id, title, artist, femaleSinger, timesPlayed) values (?, ?, ?, ?, ?)"

getSongs :: Query Rows () (UUID, ByteString, Text, Bool, Int)
getSongs = "select id, title, artist, femaleSinger, timesPlayed from songs"

getOneSong :: Query Rows UUID (Text, Int)
getOneSong = "select artist, timesPlayed from songs where id=?"

main = do
    pool <- createCassandraPool [("localhost", "9042")] "test" -- servers, keyspace
    runCas pool $ do
        do
            liftIO . print =<< executeSchema QUORUM dropSongs ()
          `catch` \exc -> case exc of
            ConfigError _ -> return ()  -- Ignore the error if the table doesn't exist
            _             -> liftIO $ throw exc

        liftIO . print =<< executeSchema QUORUM createSongs ()

        u1 <- liftIO randomIO
        u2 <- liftIO randomIO
        u3 <- liftIO randomIO
        executeWrite QUORUM insertSong (u1, "La Grange", "ZZ Top", False, 2)
        executeWrite QUORUM insertSong (u2, "Your Star", "Evanescence", True, 799)
        executeWrite QUORUM insertSong (u3, "Angel of Death", "Slayer", False, 50)

        songs <- execute QUORUM getSongs ()
        liftIO $ forM_ songs $ \(uuid, title, artist, female, played) -> do
            putStrLn ""
            putStrLn $ "id            : "++show uuid
            putStrLn $ "title         : "++C.unpack title
            putStrLn $ "artist        : "++T.unpack artist
            putStrLn $ "female singer : "++show female
            putStrLn $ "times played  : "++show played

        liftIO $ putStrLn ""
        liftIO . print =<< execute QUORUM getOneSong u2
