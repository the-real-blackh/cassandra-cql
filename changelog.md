# Changelog for [`cassandra-cql` package](https://hackage.haskell.org/package/cassandra-cql-0.5.0.0)

## 0.6
 * BREAKING: The ByteString type now gets mapped to Cassandra blobs instead
   of ASCII. Please update all your use of (ByteString/'ascii') to use the
   Ascii newtype wrapper. The Blob newtype has been removed, because it
   introduces no new meaning.

## 0.5.0.2
 * Fix incorrect upper bound for base.

## 0.5.0.1
  * Upgrade to CQL Binary Protocol v2.
  * Support Cassandra lightweight transactions.

## 0.4.0.1
  * Add PasswordAuthenticator (thanks Curtis Carter)
  * Accept ghc-7.8

## 0.3.0.1
  * Fix socket issue on Mac.
