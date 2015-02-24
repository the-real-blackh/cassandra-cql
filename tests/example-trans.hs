{-# LANGUAGE OverloadedStrings, DataKinds #-}

import Database.Cassandra.CQL
import Control.Monad
import Control.Monad.CatchIO
import Control.Monad.Trans (liftIO)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as C
import Data.Text (Text)
import qualified Data.Text as T
import Data.UUID
import System.Random

dropSongs :: Query Schema () ()
dropSongs = "drop table songs"

createSongs :: Query Schema () ()
createSongs = "create table songs (id uuid PRIMARY KEY, title ascii, artist varchar, femaleSinger boolean, timesPlayed int, comment text)"

insertSong :: Query Write (UUID, ByteString, Text, Bool, Int, Maybe Text) ()
insertSong = "insert into songs (id, title, artist, femaleSinger, timesPlayed, comment) values (?, ?, ?, ?, ?, ?) if not exists"

getSongs :: Query Rows () (UUID, ByteString, Text, Bool, Int, Maybe Text)
getSongs = "select id, title, artist, femaleSinger, timesPlayed, comment from songs"

getOneSong :: Query Rows UUID (Text, Int)
getOneSong = "select artist, timesPlayed from songs where id=?"

updateTimesPlayed :: Query Write (Int, UUID, Int) ()
updateTimesPlayed = "update songs set timesPlayed = ? where id= ? if timesPlayed = ?"

ignoreDropFailure :: Cas () -> Cas ()
ignoreDropFailure code = code `catch` \exc -> case exc of
    ConfigError _ _ -> return ()  -- Ignore the error if the table doesn't exist
    Invalid _ _ -> return ()
    _               -> throw exc

main = do
    --let auth = Just (PasswordAuthenticator "cassandra" "cassandra")
    let auth = Nothing
    {-
    Assuming a 'test' keyspace already exists. Here's some CQL to create it:
    CREATE KEYSPACE test WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };
    -}
    pool <- newPool [("localhost", "9042")] "test" auth -- servers, keyspace, maybe auth
    runCas pool $ do
        ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM dropSongs ()
        liftIO . print =<< executeSchema QUORUM createSongs ()

        u1 <- liftIO randomIO
        u2 <- liftIO randomIO
        u3 <- liftIO randomIO

        insertSongTxn u1 "La Grange" "ZZ Top" False 2 Nothing
        insertSongTxn u1 "La Grange" "ZZ Top" False 2 Nothing
        insertSongTxn u2 "Your Star" "Evanescence" True 799 Nothing
        insertSongTxn u2 "Your Star" "Evanescence" True 799 Nothing
        insertSongTxn u3 "Angel of Death" "Slayer" False 50 (Just "Singer Tom Araya")
        insertSongTxn u3 "Angel of Death" "Slayer" False 50 (Just "Singer Tom Araya")

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
        liftIO . print =<< executeRow QUORUM getOneSong u2

        res <- executeTrans updateTimesPlayed (800,u2,799)
        liftIO $ putStrLn $ "Update timesPlayed 799 to 800 : success"
        liftIO . print =<< executeRow QUORUM getOneSong u2

        res <- executeTrans updateTimesPlayed (800,u2,799)
        when (not res) $ liftIO $ putStrLn $ "Update timesPlayed 799 to 800 : failed!"

        res <- executeTrans updateTimesPlayed (801,u2,800)
        liftIO $ putStrLn $ "Update timesPlayed 800 to 801 : success"
        liftIO . print =<< executeRow QUORUM getOneSong u2

    where
      insertSongTxn id title artist femaleSinger timesPlayed comment = do
        res <- executeTrans insertSong (id, title, artist, femaleSinger, timesPlayed, comment)
        when (not res) $ liftIO $ putStrLn $ "Song " ++ show id ++ " already exists."
