CREATE KEYSPACE cssArchive;
USE cssArchive;
CREATE COLUMN FAMILY engineConfiguration;
CREATE COLUMN FAMILY engineConfigurationToGroups;
CREATE COLUMN FAMILY groupConfiguration;
CREATE COLUMN FAMILY groupConfigurationToChannels;
CREATE COLUMN FAMILY channelConfiguration;
CREATE COLUMN FAMILY channelConfigurationToCompressionLevels;
CREATE COLUMN FAMILY compressionLevelConfiguration;
CREATE COLUMN FAMILY samples WITH
  compression_options = {
    sstable_compression: DeflateCompressor,
    chunk_length_kb: 256
  };
