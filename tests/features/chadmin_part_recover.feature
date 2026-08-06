Feature: Recover data from broken detached object-storage parts

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse01

  @require_version_25.8
  Scenario: Recover a Wide part by inferred and explicit source table
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS recovery_source;
    DROP DATABASE IF EXISTS recovered_by_path;
    DROP DATABASE IF EXISTS recovered_by_name;
    DROP DATABASE IF EXISTS recovered_by_table;

    CREATE DATABASE recovery_source;
    CREATE TABLE recovery_source.source
    (
        id UInt64,
        keep String,
        lost String
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    SETTINGS
        storage_policy = 'object_storage',
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0;

    INSERT INTO recovery_source.source VALUES
        (1, 'one', 'lost-one'),
        (2, 'two', 'lost-two'),
        (3, 'three', 'lost-three');

    ALTER TABLE recovery_source.source DETACH PARTITION tuple();
    """
    And we remove S3 blobs for file lost.bin from detached part recovery_source.source on clickhouse01

    When we execute command on clickhouse01
    """
    PART_PATH=$(clickhouse client --query "
        SELECT path
        FROM system.detached_parts
        WHERE database = 'recovery_source' AND table = 'source'
    ")
    chadmin --format yaml part recover --path "$PART_PATH" --target-table recovered_by_path.data
    """
    Then we get response contains
    """
    source_column: lost
    """
    And we get response contains
    """
    status: lost
    """

    When we execute query on clickhouse01
    """
    SELECT name
    FROM system.columns
    WHERE database = 'recovered_by_path' AND table = 'data'
    ORDER BY position
    """
    Then we get response
    """
    id
    keep
    """

    When we execute query on clickhouse01
    """
    SELECT id, keep
    FROM recovered_by_path.data
    ORDER BY id
    """
    Then we get query response
    """
    1\tone
    2\ttwo
    3\tthree
    """

    When we execute command on clickhouse01
    """
    PART_NAME=$(clickhouse client --query "
        SELECT name
        FROM system.detached_parts
        WHERE database = 'recovery_source' AND table = 'source'
    ")
    chadmin part recover --database recovery_source --table source --name "$PART_NAME" --target-table recovered_by_name.data
    """

    When we execute query on clickhouse01
    """
    SELECT id, keep
    FROM recovered_by_name.data
    ORDER BY id
    """
    Then we get query response
    """
    1\tone
    2\ttwo
    3\tthree
    """

    When we execute command on clickhouse01
    """
    PART_PATH=$(clickhouse client --query "
        SELECT path
        FROM system.detached_parts
        WHERE database = 'recovery_source' AND table = 'source'
    ")
    DISK_PATH=$(clickhouse client --query "
        SELECT path FROM system.disks WHERE name = 'object_storage'
    ")
    RECOVERY_PATH="${DISK_PATH%/}/recovery-input/broken_part"
    mkdir -p "${RECOVERY_PATH%/*}"
    mv "$PART_PATH" "$RECOVERY_PATH"

    chadmin part recover --path "$RECOVERY_PATH" --database recovery_source --table source --target-table recovered_by_table.data
    """

    When we execute query on clickhouse01
    """
    SELECT id, keep
    FROM recovered_by_table.data
    ORDER BY id
    """
    Then we get query response
    """
    1\tone
    2\ttwo
    3\tthree
    """

    When we execute command on clickhouse01
    """
    DISK_PATH=$(clickhouse client --query "
        SELECT path FROM system.disks WHERE name = 'object_storage'
    ")
    RECOVERY_PATH="${DISK_PATH%/}/recovery-input/broken_part"
    chadmin --format yaml part recover --path "$RECOVERY_PATH" --database recovery_source --table source
    """
    Then we get response contains
    """
    target_table: recovery_source._chadmin_recovered_
    """

    When we execute query on clickhouse01
    """
    SELECT id, keep
    FROM merge('recovery_source', '^_chadmin_recovered_')
    ORDER BY id
    """
    Then we get query response
    """
    1\tone
    2\ttwo
    3\tthree
    """

  @require_version_25.8
  Scenario: Recover a Compact part with broken regenerable metadata
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS compact_recovery_source;
    DROP DATABASE IF EXISTS recovered_compact;

    CREATE DATABASE compact_recovery_source;
    CREATE TABLE compact_recovery_source.source
    (
        id UInt64,
        value String
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    SETTINGS
        storage_policy = 'object_storage',
        min_bytes_for_wide_part = 1000000000,
        min_rows_for_wide_part = 1000000000;

    INSERT INTO compact_recovery_source.source VALUES
        (1, 'one'),
        (2, 'two');

    ALTER TABLE compact_recovery_source.source DETACH PARTITION tuple();
    """
    And we remove S3 blobs for file checksums.txt from detached part compact_recovery_source.source on clickhouse01

    When we execute command on clickhouse01
    """
    PART_NAME=$(clickhouse client --query "
        SELECT name FROM system.detached_parts
        WHERE database = 'compact_recovery_source' AND table = 'source'
    ")
    chadmin part recover --database compact_recovery_source --table source --name "$PART_NAME" --target-table recovered_compact.data
    """

    When we execute query on clickhouse01
    """
    SELECT id, value FROM recovered_compact.data ORDER BY id
    """
    Then we get query response
    """
    1\tone
    2\ttwo
    """

  @require_version_less_than_25.8
  Scenario: Reject part recovery on an unsupported ClickHouse version
    When we try to execute command on clickhouse01
    """
    chadmin part recover --database default --table source --name all_1_1_0 --target-table recovered.data
    """
    Then it fails with response contains
    """
    Part recovery requires ClickHouse version 25.8 or above
    """
